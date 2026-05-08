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

8. GOLM / MoM GROWTH: Calculate 'Growth Over Last Month' using LAG:
   SELECT month_name, total_amount, 
   (total_amount - LAG(total_amount) OVER (ORDER BY year_number, month_number)) / NULLIF(LAG(total_amount) OVER (ORDER BY year_number, month_number), 0) * 100 as "GOLM%"
   FROM (SELECT year_number, month_number, month_name, SUM(total_amount) as total_amount FROM master_sale GROUP BY 1,2,3) sub;

9. YoY GROWTH: Calculate 'Year Over Year' by comparing same month across years:
   LAG(total_amount, 12) OVER (ORDER BY year_number, month_number)
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


# ---------------------------------------------------------------------------10. CURRENCY: ALWAYS use 'PKR' or 'Rs.' for any monetary values. NEVER use '$'. All sales data is in Pakistani Rupees.
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


def generate_map_sql(
    question: str,
    history: List[Dict[str, str]] | None = None,
) -> Dict[str, Any]:
    """
    SPECIALIZED AGENT FOR MAPS.
    Focuses exclusively on fetching latitude, longitude, name, and sales data.
    """
    history = history or []
    
    MAP_EXPERT_PROMPT = f"""You are a specialized Geospatial Data Agent for a Pharmaceutical Dashboard.
Your ONLY job is to generate SQL that returns coordinates and sales data for the Map UI.

CORE SCHEMA:
{_SCHEMA}

STRICT MAPPING RULES:
1. MANDATORY COLUMNS: You MUST always include "latitude" (float), "longitude" (float), "name" (string), and "address" (string).
2. SALES INTEGRATION: Always JOIN with "master_sale" to include sales metrics (SUM("total_amount") as "total_sales").
3. ENTITY TYPES: 
   - If user asks for pharmacies/customers: Use table "customers".
   - If user asks for doctors/health centres: Use table "healthcentres".
4. IDENTIFIER MATCHING: 
   - Use ILIKE for name searches (e.g. "name" ILIKE '%Bismillah%').
5. NO CONVERSATION: Return ONLY SQL inside ```sql ... ``` block. No explanations.
6. ERROR: If you cannot find a way to get coordinates, return 'ERROR: No coordinates available for this entity.'

Example:
SELECT c.name, c.latitude, c.longitude, c.address, SUM(ms.total_amount) as total_sales
FROM customers c 
JOIN master_sale ms ON c.id = ms.customer_id
WHERE c.name ILIKE '%LIAQUATABAD%'
GROUP BY 1, 2, 3, 4
"""

    messages = [{"role": "system", "content": MAP_EXPERT_PROMPT}]
    for turn in history[-4:]:
        messages.append(turn)
    messages.append({"role": "user", "content": question})

    last_error: str | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1 and last_error:
            messages.append({"role": "user", "content": f"Previous SQL failed: {last_error}. Fix and retry."})

        try:
            resp = client.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0,
                timeout=30
            )
            raw = resp.choices[0].message.content.strip()
            if "ERROR:" in raw: return {"sql": raw, "results": None, "error": raw, "retries": attempt}
            sql = _extract_sql(raw)
            messages.append({"role": "assistant", "content": raw})
            results = run_sql_query(sql)
            if isinstance(results, list):
                return {"sql": sql, "results": results, "error": None, "retries": attempt}
            last_error = results.get("error", "Unknown error")
        except Exception as exc:
            if attempt < MAX_RETRIES: continue
            return {"sql": "", "results": None, "error": str(exc), "retries": attempt}

    return {"sql": "", "results": None, "error": last_error, "retries": MAX_RETRIES}


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

RULE 1 — ALWAYS INCLUDE NAME COLUMNS:
  When querying doctors, managers, health centres, customers, products, or any entity table,
  you MUST always include the human-readable name column (e.g. "name", "doctor_name") in the SELECT.
  NEVER return only numeric IDs. Always JOIN to get names if the ID is in a fact table.
  Example: SELECT d.name, COUNT(*) FROM doctor_plan dp JOIN doctors d ON d.id = dp."doctorId" GROUP BY d.name

RULE 2 — NO COUNT HALLUCINATION:
  NEVER state "there are X records in total" unless you explicitly ran a COUNT(*) query.
  If you used LIMIT N, only describe the N records returned. Do not guess the total.

RULE 3 — CONTEXTUAL FILTERING (CRITICAL):
  If the user uses pronouns or references like "iske", "unka", "uska", "in ka", "is",
  "him", "her", "they", "their", "same", "ye", "wo", "iska", or asks a follow-up
  without specifying an entity name — you MUST identify the entity from HISTORY
  (the previous user question or assistant response) and apply the SAME filter.
  Example: User asks "Farman Ali ka data" → next message "iske visits?" → filter by "FARMAN ALI".

RULE 4 — NEVER EXPOSE SENSITIVE COLUMNS:
  NEVER select "password", "refresh_token", "token", "secret" columns in any query.
  Only select columns that are directly relevant to the user's question.

RULE 5 — MAP / LOCATION QUERIES (lat/lng mandatory):
  If the user asks to "show on map", "map pr dikhao", "location", or uses map-related words:
  You MUST include columns "latitude" (as float) and "longitude" (as float) in your SELECT.
  - For Customers/Pharmacies: JOIN "customers" with "master_sale" to get location and sales.
  - For Doctors/HCs: JOIN "healthcentres" with "manager_calls" or "doctor_plan".
  The UI automatically renders a map if these columns are present.
  NEVER omit latitude and longitude for map-related requests.

RULE 6 — AUTOMATIC SALES DATA:
  Whenever the user asks to list or show pharmacies, customers, or sales-related entities,
  you MUST automatically JOIN with 'master_sale' (internal) or 'ims_sale' (market)
  to include sales metrics (e.g. SUM(total_amount) or SUM(product_quantity)).
  Even if the user only asks for a list, providing their sales performance is mandatory.

RULE 7 — MAP AND SALES INTEGRATION (CRITICAL):
  When the user asks to "show on map" or "location", you MUST provide both location
  (latitude, longitude) AND sales data (total_amount) in the same query whenever possible.
  If the user asks for multiple entities (e.g. "show all Bismillah customers on map"),
  return ALL matching records in a single query result so they can be plotted together.
  NEVER return them one-by-one.

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
                timeout=30, # Increased timeout for stability
            )
            raw = resp.choices[0].message.content.strip()
        except Exception as exc:
            if attempt < MAX_RETRIES:
                last_error = f"Connection/Network error: {str(exc)}. Retrying..."
                continue
            return {"sql": "", "results": None, "error": f"Connection error after {MAX_RETRIES} attempts: {str(exc)}", "retries": attempt}

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
        "IMPORTANT: ALWAYS use 'Rs.' or 'PKR' for currency. NEVER use '$'. "
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
        "IMPORTANT: ALWAYS use 'Rs.' or 'PKR' for currency. NEVER use '$'. "
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
