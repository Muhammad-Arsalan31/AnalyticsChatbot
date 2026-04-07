from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import re
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import openai
import json
import decimal

# --- CONFIGURATION ---
load_dotenv(override=True)
DB_URL = os.getenv("DATABASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")

client = openai.OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

app = FastAPI(title="Pharma AI Agent API")

@app.get("/")
async def root():
    return {"status": "online", "message": "Pharma AI Agent API is running. Access /docs for documentation."}

class QueryRequest(BaseModel):
    prompt: str
    username: str = "default_user"

# Custom JSON encoder for Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

def run_sql(query: str):
    try:
        clean_url = DB_URL.split('?')[0] if DB_URL else ""
        conn = psycopg2.connect(clean_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchall()
            return res
    except Exception as e:
        return {"error": str(e)}

# --- INTELLIGENCE HELPERS ---
def get_schema():
    try:
        with open("prisma/schema.prisma", "r") as f:
            return f.read()
    except:
        return "Schema unavailable."

def get_rag_context():
    context = ""
    knowledge_dir = "knowledge"
    if os.path.exists(knowledge_dir):
        for filename in os.listdir(knowledge_dir):
            if filename.endswith(".md"):
                with open(os.path.join(knowledge_dir, filename), "r") as f:
                    context += f"\n--- From {filename} ---\n{f.read()}\n"
    
    context += "\n--- MASTER SQL RULES ---\n"
    context += "1. USE 'master_sale' FOR INTERNAL METRICS (Sales, Qty).\n"
    context += "2. USE 'ims_sale' FOR MARKET DATA.\n"
    context += "3. For bricks, use 'ims_brick' table.\n"
    context += "4. ALWAYS use ILIKE '%value%' for strings.\n"
    return context

@app.post("/ask")
async def ask_agent(request: QueryRequest):
    prompt = request.prompt
    schema = get_schema()
    rag = get_rag_context()
    
    sys_prompt = "You are a professional Pharma SQL Expert. Use the provided schema and knowledge base. RETURN ONLY SQL."
    gen_prompt = (
        f"KNOWLEDGE BASE:\n{rag}\n\n"
        f"SCHEMA:\n{schema}\n\n"
        f"USER QUESTION: {prompt}\n"
        "Return PostgreSQL inside ```sql blocks."
    )
    
    try:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": gen_prompt}],
            timeout=30.0
        )
        content = response.choices[0].message.content
        sql_match = re.search(r"```sql\n(.*?)\n```", content, re.DOTALL)
        sql_query = sql_match.group(1).strip() if sql_match else content.strip()
        
        results = run_sql(sql_query)
        if isinstance(results, dict) and "error" in results:
            return {"status": "error", "error": results["error"], "attempted_sql": sql_query}
        
        sum_prompt = f"User: {prompt}\nData: {results}\nSummarize strategically in Roman Urdu/English (Max 3 bullets)."
        summary_res = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": sum_prompt}]
        )
        summary = summary_res.choices[0].message.content

        return {
            "status": "success",
            "sql": sql_query,
            "data": json.loads(json.dumps(results, cls=DecimalEncoder)),
            "summary": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
