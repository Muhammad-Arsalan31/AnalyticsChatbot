"""
agent_core.py — LLM + SQL orchestration layer
Fixes / Features:
  - RAG context loaded from knowledge/ (was missing in CLI)
  - Schema cached at module level (no repeated disk reads)
  - SQL self-correction retry loop (up to MAX_RETRIES attempts)
  - Streaming support via generator
  - Conversation history support for follow-up questions
"""

import os
import re
from typing import Generator, List, Dict, Any

import openai
from dotenv import load_dotenv

from db import run_sql_query

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LLM_API_KEY  = (os.getenv("LLM_API_KEY") or "").strip()
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL    = os.getenv("LLM_MODEL",    "meta-llama/llama-3.3-70b-instruct")

client = openai.OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

MAX_RETRIES = 3   # SQL self-correction attempts

# ---------------------------------------------------------------------------
# Schema & RAG — cached at module load (no per-call disk I/O)
# ---------------------------------------------------------------------------
def _load_schema() -> str:
    path = os.path.join(os.getcwd(), "prisma", "schema.prisma")
    try:
        with open(path, "r") as f:
            return f.read()
    except Exception:
        return "Schema file not found."


def _load_rag_context() -> str:
    """Load all .md files from knowledge/ and append hard-coded SQL rules."""
    context = ""
    knowledge_dir = os.path.join(os.getcwd(), "knowledge")
    if os.path.exists(knowledge_dir):
        for fname in sorted(os.listdir(knowledge_dir)):
            if fname.endswith(".md"):
                fpath = os.path.join(knowledge_dir, fname)
                try:
                    with open(fpath, "r") as f:
                        context += f"\n--- From {fname} ---\n{f.read()}\n"
                except Exception:
                    pass

    context += """
--- MASTER SQL RULES (MANDATORY) ---
1. USE 'master_sale' FOR INTERNAL METRICS: columns are product_name, product_quantity,
   total_amount, region_name, zone_name, area_name, invoice_date, month_number,
   year_number, month_name.
   NOTE: 'sale_date' does NOT exist — use 'invoice_date' for full date.

2. JOINING BRICK NAMES: 'master_sale' has no brick_name. For brick-level internal
   sales use:
     SELECT ib.name, SUM(ms.product_quantity)
     FROM master_sale ms
     JOIN customer_details cd ON ms.customer_id = cd.customer_id
     JOIN ims_brick ib ON cd.ims_brick_id = ib.id
     GROUP BY ib.name

3. NO CARTESIAN PRODUCTS: Never join 'ims_sale' and 'invoice_details' (or
   'master_sale') directly. Use CTEs:
     WITH Internal AS (
       SELECT ib.name, SUM(ms.product_quantity) as qty
       FROM master_sale ms
       JOIN customer_details cd ON ms.customer_id = cd.customer_id
       JOIN ims_brick ib ON cd.ims_brick_id = ib.id
       GROUP BY 1
     ),
     Market AS (
       SELECT ib.name, SUM(unit) as qty
       FROM ims_sale s JOIN ims_brick ib ON s."brickId" = ib.id
       GROUP BY 1
     )
     SELECT i.name, i.qty AS "Internal", m.qty AS "Market"
     FROM Internal i JOIN Market m ON i.name = m.name

4. FUZZY SEARCH: For all user filters use ILIKE with wildcards:
   WHERE ib.name ILIKE '%gulshan%'

5. GULSHAN AGGREGATION: Always use ILIKE '%gulshan%' to capture all blocks.

6. EMPTY TABLES: 'orders' and 'targets' are EMPTY. Use 'master_sale' or
   'invoice_details' for sales data.

7. ALWAYS double-quote table and column names to avoid case-sensitivity errors,
   e.g. "doctor_plan"."managerId".
"""
    return context


# Cached at module import — never re-read from disk during a session
_SCHEMA: str      = _load_schema()
_RAG_CONTEXT: str = _load_rag_context()


# ---------------------------------------------------------------------------
# SQL extraction helper
# ---------------------------------------------------------------------------
def _extract_sql(text: str) -> str:
    match = re.search(r"```sql\n(.*?)\n```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Fallback: return raw text (may still be valid SQL)
    return text.strip()


# ---------------------------------------------------------------------------
# Core: generate SQL with self-correction retry loop
# ---------------------------------------------------------------------------
def _is_web_search_intent(text: str) -> bool:
    """Detect if the query specifically asks for internet/latest information."""
    keywords = ["google", "search", "latest news", "internet", "web search", "tavily", "current events"]
    t_lower = text.lower()
    return any(k in t_lower for k in keywords)


def web_search_tavily(query: str) -> str:
    """Execute a real-time web search and return results/summary."""
    tavily_api_key = os.getenv("TAVILY_API_KEY")
    if not tavily_api_key:
        return "ERROR: Tavily API Key missing in .env."

    try:
        from tavily import TavilyClient
        tavily = TavilyClient(api_key=tavily_api_key)
        search_result = tavily.search(query=query, search_depth="advanced")
        if search_result.get('results'):
            reply = f"🌐 **Web Insights for: '{query}'**\n\n"
            for res in search_result['results'][:3]:
                reply += f"- [{res['title']}]({res['url']})\n"
            reply += f"\n\n**Summary:** {search_result['results'][0]['content'][:1000]}"
            return reply
        return "No relevant web search results found."
    except Exception as e:
        return f"⚠️ Web search failed (Network Error): {str(e)}"


def generate_and_run_sql(
    question: str,
    history: List[Dict[str, str]] | None = None,
) -> Dict[str, Any]:
    """
    1. Ask LLM to generate SQL (with schema + RAG context).
    2. Execute SQL.
    3. On error, feed the error back to the LLM and retry (up to MAX_RETRIES).
    Returns:
      {
        "sql":     str,
        "results": list[dict] | None,
        "error":   str | None,
        "retries": int,
      }
    """
    history = history or []

    # 0. Check for Web Search Intent first
    if _is_web_search_intent(question):
        web_res = web_search_tavily(question)
        return {
            "sql": "",
            "results": [{"web_report": web_res}],
            "is_conversational": True,
            "is_web": True,
            "error": None,
            "retries": 0
        }

    system_prompt = f"""You are a production-grade Text-to-SQL AI Agent for PostgreSQL.

SCHEMA:
{_SCHEMA}

BUSINESS CONTEXT & SQL RULES:
{_RAG_CONTEXT}

STRICT OUTPUT RULES:
- Output ONLY the SQL query inside a ```sql ... ``` code block.
- Use only READ-ONLY SELECT / WITH queries.
- Always double-quote identifiers.
- Use LIMIT 10 unless the user specifies otherwise.
- CONTEXTUAL FILTERING (CRITICAL): If the user asks a follow-up question (e.g., "where did he visit?", "now show sales for them"), you MUST identify the referenced entities (Manager Name, Doctor, Product, Area) from the provided HISTORY and apply the same filters to your new SQL query.
- If the requested data does not exist in the schema respond with:
  ERROR: Data not available in database.
"""

    messages = [{"role": "system", "content": system_prompt}]

    # Include recent conversation turns for follow-up support
    for turn in history[-6:]:   # last 3 Q&A pairs
        messages.append(turn)

    messages.append({"role": "user", "content": question})

    last_error: str | None = None
    sql: str = ""

    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1 and last_error:
            # Self-correction: tell the LLM what went wrong
            correction_msg = (
                f"The previous SQL query failed with this error:\n\n"
                f"  {last_error}\n\n"
                f"Please fix the query and return ONLY the corrected SQL in a ```sql``` block."
            )
            messages.append({"role": "user", "content": correction_msg})

        try:
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0,
            )
            raw = resp.choices[0].message.content.strip()
        except Exception as exc:
            return {"sql": "", "results": None, "error": str(exc), "retries": attempt}

        if "ERROR:" in raw and "```sql" not in raw:
            return {"sql": raw, "results": None, "error": raw, "retries": attempt}

        sql = _extract_sql(raw)

        # Append assistant reply so self-correction has the faulty query in context
        messages.append({"role": "assistant", "content": raw})

        results = run_sql_query(sql)

        # Success
        if isinstance(results, list):
            return {"sql": sql, "results": results, "error": None, "retries": attempt}

        # Error returned — loop for self-correction
        last_error = results.get("error", "Unknown error")

    # All retries exhausted
    return {"sql": sql, "results": None, "error": last_error, "retries": MAX_RETRIES}


# ---------------------------------------------------------------------------
# Summarise results — plain text (non-streaming)
# ---------------------------------------------------------------------------
def summarise_results(question: str, results: list) -> str:
    prompt = (
        f"The user asked: {question}\n\n"
        f"The database returned: {results}\n\n"
        "Convert this into a clear, concise, business-friendly natural language answer. "
        "Do NOT mention SQL or technical database terms. "
        "Use bullet points for lists. Be direct and analytical."
    )
    try:
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        return f"(Summary unavailable: {exc})"


# ---------------------------------------------------------------------------
# Summarise results — STREAMING generator
# ---------------------------------------------------------------------------
def summarise_results_stream(
    question: str, results: list
) -> Generator[str, None, None]:
    """
    Yields text chunks as they arrive from the LLM.
    Usage (Streamlit):
        with st.chat_message("assistant"):
            response = st.write_stream(summarise_results_stream(q, rows))
    """
    prompt = (
        f"The user asked: {question}\n\n"
        f"The database returned: {results}\n\n"
        "Convert this into a clear, concise, business-friendly natural language answer. "
        "Do NOT mention SQL or technical database terms. "
        "Use bullet points for lists. Be direct and analytical."
    )
    try:
        stream = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )
        for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta
    except Exception as exc:
        yield f"\n\n(Streaming error: {exc})"


# ---------------------------------------------------------------------------
# Suggest follow-up questions
# ---------------------------------------------------------------------------
def suggest_followups(question: str, results: list) -> List[str]:
    prompt = (
        f"User asked: {question}\n"
        f"Data returned had {len(results)} rows.\n\n"
        "Suggest exactly 3 short, specific follow-up questions the user might want to ask next. "
        "Output ONLY a Python list of 3 strings, nothing else. Example:\n"
        '["Question 1?", "Question 2?", "Question 3?"]'
    )
    try:
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        raw = resp.choices[0].message.content.strip()
        import ast
        suggestions = ast.literal_eval(raw)
        if isinstance(suggestions, list):
            return suggestions[:3]
    except Exception:
        pass
    return []
