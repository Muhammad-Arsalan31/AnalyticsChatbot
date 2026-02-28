import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import openai
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

# Configuration
DB_URL = os.getenv("DATABASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
# Map common display names to OpenRouter slugs if necessary
RAW_MODEL = os.getenv("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")
if "Llama 3.3 70B Instruct" in RAW_MODEL:
    LLM_MODEL = "meta-llama/llama-3.3-70b-instruct"
else:
    LLM_MODEL = RAW_MODEL

# Initialize OpenAI/OpenRouter client
client = openai.OpenAI(
    api_key=LLM_API_KEY,
    base_url=LLM_BASE_URL
)

def run_sql_query(query: str) -> List[Dict[str, Any]]:
    """Executes a READ-ONLY SQL query against the database."""
    # Strict safety check
    forbidden_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE"]
    query_upper = query.upper()
    
    if not query_upper.strip().startswith("SELECT"):
        return [{"error": "Only SELECT queries are allowed."}]
    
    for word in forbidden_keywords:
        if word in query_upper:
            # Check if it's a standalone word to avoid false positives (e.g. table name 'deleted_at')
            if re.search(rf'\b{word}\b', query_upper):
                return [{"error": f"Keyword {word} is not allowed."}]

    conn = None
    try:
        # Clean URL (psycopg2 might not like ?schema=... params)
        clean_url = DB_URL.split('?')[0] if DB_URL else ""
        conn = psycopg2.connect(clean_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            return results
    except Exception as e:
        return [{"error": str(e)}]
    finally:
        if conn:
            conn.close()

def get_schema_context():
    """Reads the schema file to provide context for the LLM."""
    schema_path = os.path.join(os.getcwd(), "prisma", "schema.prisma")
    try:
        with open(schema_path, "r") as f:
            return f.read()
    except:
        return "Schema file not found."

def ask_agent(question: str):
    schema = get_schema_context()
    
    # 1. Generate SQL
    prompt = f"""
    You are a production-grade Text-to-SQL AI Agent for PostgreSQL.
    
    SCHEMA:
    {schema}
    
    RULES:
    - IMPORTANT: Always use double quotes for ALL table and column names (e.g., "doctor_plan"."managerId") to avoid case-sensitivity errors.
    - ONLY output the SQL query inside a code block.
    - Use READ-ONLY SELECT queries.
    - Answer ONLY using the schema provided.
    - If required data is not in schema, Respond with "ERROR: Data not available in database."
    - Use aliases for readability.
    - Use LIMIT 10 if not specified.
    
    USER QUESTION: {question}
    """
    
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "system", "content": "You are a professional SQL expert."},
                  {"role": "user", "content": prompt}]
    )
    
    sql_match = re.search(r"```sql\n(.*?)\n```", response.choices[0].message.content, re.DOTALL)
    if not sql_match:
        sql_query = response.choices[0].message.content.strip()
    else:
        sql_query = sql_match.group(1).strip()

    if "ERROR" in sql_query:
        print(f"\n[Agent]: {sql_query}")
        return

    print(f"\n[SQL Generated]:\n{sql_query}")

    # 2. Execute SQL
    results = run_sql_query(sql_query)
    
    if results and "error" in results[0]:
        print(f"\n[Error]: {results[0]['error']}")
        return

    # 3. Summarize Results
    summary_prompt = f"""
    The user asked: {question}
    The database returned: {results}
    
    Convert this result into a clear, concise, business-friendly natural language answer.
    Do NOT mention SQL or technical database terms.
    """
    
    summary_res = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": summary_prompt}]
    )
    
    print(f"\n[Answer]: {summary_res.choices[0].message.content}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        user_input = " ".join(sys.argv[1:])
        ask_agent(user_input)
    else:
        print("Usage: python agent.py \"your question here\"")
