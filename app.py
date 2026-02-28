import streamlit as st
import pandas as pd
import plotly.express as px
import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import openai
from typing import List, Dict, Any

# --- CONFIGURATION ---
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
RAW_MODEL = os.getenv("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")
LLM_MODEL = "meta-llama/llama-3.3-70b-instruct" if "Llama 3.3 70B Instruct" in RAW_MODEL else RAW_MODEL

client = openai.OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

import json

CACHE_FILE = "query_cache.json"

def load_query_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_to_query_cache(question, sql):
    cache = load_query_cache()
    normalized_q = question.lower().strip()
    cache[normalized_q] = sql
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=4)

# --- DATABASE ENGINE ---
def run_sql_query(query: str):
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
    query_u = query.upper()
    if not query_u.strip().startswith("SELECT"):
        return {"error": "Only SELECT queries are allowed."}
    for word in forbidden:
        if re.search(rf'\b{word}\b', query_u):
            return {"error": f"Keyword {word} is not allowed."}

    try:
        clean_url = DB_URL.split('?')[0] if DB_URL else ""
        conn = psycopg2.connect(clean_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        return {"error": str(e)}

def get_schema():
    try:
        with open("prisma/schema.prisma", "r") as f:
            return f.read()
    except:
        return "Schema unavailable."

def get_rag_context(user_query: str):
    """Retrieves relevant business logic and SQL examples."""
    context = ""
    knowledge_dir = "knowledge"
    if os.path.exists(knowledge_dir):
        for filename in os.listdir(knowledge_dir):
            if filename.endswith(".md"):
                with open(os.path.join(knowledge_dir, filename), "r") as f:
                    content = f.read()
                    context += f"\n--- From {filename} ---\n{content}\n"
    
    # Add explicit warning about table existence and column mapping based on schema analysis
    context += "\n--- DATABASE SCHEMA MAPPINGS & LIMITATIONS (DO NOT IGNORE) ---\n"
    context += "- WARNING: Table 'orders' does NOT contain 'product_quantity'. DO NOT use o.\"product_quantity\".\n"
    context += "- RULE: Join 'orders' -> 'order_details' to get 'product_quantity'.\n"
    context += "- RULE: Always CAST \"order_details\".\"product_quantity\" to NUMERIC (e.g. SUM(CAST(\"order_details\".\"product_quantity\" AS NUMERIC))).\n"
    context += "- TABLE 'doctors': Use 'category' for doctor segments (A, B, C, D).\n"
    context += "- TABLE 'doctor_calls': DOES NOT EXIST. Use 'doctor_plan' for all visit-related queries.\n"
    context += "- TABLE 'targets': Use this for sales goals. Columns: \"quantity_target\", \"sale_target\".\n"
    context += "- TABLE 'ims_sale' / 'ims_customer_sale': Use \"unit\" for quantity and \"price\" for value.\n"
    return context

# --- PAGE CONFIG ---
st.set_page_config(page_title="Antigravity Pharma AI", page_icon="💊", layout="wide")

st.markdown("""
<style>
    .main { background-color: #0e1117; color: #ffffff; }
    .stTextInput > div > div > input { background-color: #262730; color: white; border-radius: 10px; }
    .stChatMessage { border-radius: 15px; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

st.title("💊 Antigravity Pharma Intelligence")
st.caption("AI Agent with RAG (Retrieval-Augmented Generation)")

# --- SIDEBAR ---
with st.sidebar:
    st.header("📊 Data Health Check")
    st.success("IMS Market Sales: 156k+ records")
    st.success("Internal Sales (Invoices): 55k+ records")
    st.success("Doctors: 382 records")
    st.warning("⚠️ Orders & Targets: 0 records")
    
    st.divider()
    if st.button("Clear History"):
        st.session_state.messages = []
    st.divider()
    st.markdown("### Agent Reason (RAG)")
    st.write("The agent uses `knowledge/` and REAL-TIME table stats for high accuracy.")

# --- CHAT INTERFACE ---
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "data" in message:
            st.dataframe(message["data"])
        if message.get("chart_data") is not None:
            # Re-generate chart to avoid session state serialization issues
            x_col, y_cols = message["chart_data"]
            fig = px.bar(message["data"], x=x_col, y=y_cols, barmode='group', template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

if prompt := st.chat_input("Ask about your pharma data..."):
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.spinner("Consulting Knowledge Base & Database..."):
        try:
            schema = get_schema()
            rag_context = get_rag_context(prompt)
            
            # --- CACHE CHECK ---
            cache = load_query_cache()
            normalized_q = prompt.lower().strip()
            
            sql_query = ""
            results = None
            
            if normalized_q in cache:
                sql_query = cache[normalized_q]
                results = run_sql_query(sql_query)
                # If cached query fails (schema changed?), reset and try LLM
                if isinstance(results, dict) and "error" in results:
                    results = None
                    sql_query = ""

            # --- LLM GENERATION / SELF-CORRECTING LOOP (if not cached) ---
            if results is None:
                max_retries = 3
                current_attempt = 0
                last_error = ""
                
                while current_attempt < max_retries:
                    current_attempt += 1
                    error_feedback = f"\n\nLAST ERROR WAS: {last_error}" if last_error else ""
                    gen_prompt = (
                        f"KNOWLEDGE BASE & BUSINESS LOGIC:\n{rag_context}\n\n"
                        f"DATABASE SCHEMA:\n{schema}\n\n"
                        f"IMPORTANT DATA STATS (REAL-TIME):\n"
                        f"- 'invoice' and 'invoice_details' have 55,000+ rows. USE THESE FOR INTERNAL SALES.\n"
                        f"- 'orders' and 'targets' tables are EMPTY (0 rows). DO NOT USE THEM.\n"
                        f"- 'ims_sale' has 156,000+ rows. Use this for Market benchmarks.\n\n"
                        f"USER QUESTION: {prompt}"
                        f"{error_feedback}\n\n"
                        "RULES:\n"
                        "- ALWAYS use \"invoice_details\" for internal sales quantity (column \"product_quantity\").\n"
                        "- If the user asks for targets, explain that the table is empty and suggest analyzing past invoices instead.\n"
                        "- IMPORTANT: FIX the last error if provided.\n"
                        "- Return ONLY valid PostgreSQL SELECT query inside triple backticks.\n"
                        "- Use double quotes for ALL table and column names.\n"
                        "- Use LIMIT 10."
                    )
                    
                    response = client.chat.completions.create(
                        model=LLM_MODEL,
                        messages=[{"role": "system", "content": "You are a professional database agent."}, {"role": "user", "content": gen_prompt}],
                        timeout=30.0
                    )
                    
                    content = response.choices[0].message.content
                    sql_match = re.search(r"```sql\n(.*?)\n```", content, re.DOTALL)
                    sql_query = sql_match.group(1).strip() if sql_match else content.strip()

                    results = run_sql_query(sql_query)
                    
                    if isinstance(results, dict) and "error" in results:
                        last_error = results["error"]
                        continue
                    else:
                        # Save successful query to cache
                        save_to_query_cache(prompt, sql_query)
                        break

            # Final Output Handling
            if isinstance(results, dict) and "error" in results:
                st.error(f"Failed after {max_retries} attempts. Final Error: {results['error']}")
            else:
                df = pd.DataFrame(results)
                
                if df.empty:
                    final_answer = "I executed the query, but it returned no data. This usually means the specific filters (like region name) didn't match anything in the database."
                else:
                    sum_prompt = (
                        f"User asked: {prompt}\nDB Result: {results}\n\n"
                        "Task: Provide a concise (1-2 sentence) business summary of the results.\n"
                        "STRICT RULES:\n"
                        "- DO NOT lecture the user on data integrity.\n"
                        "- DO NOT suggest alternative data sources.\n"
                        "- DO NOT explain query accuracy.\n"
                        "- Just summarize the numeric findings simply."
                    )
                    summary_res = client.chat.completions.create(model=LLM_MODEL, messages=[{"role": "user", "content": sum_prompt}], timeout=30.0)
                    final_answer = summary_res.choices[0].message.content
                
                with st.chat_message("assistant"):
                    st.markdown(final_answer)
                    # st.code(sql_query, language="sql") # Optional: Show SQL for debugging
                    if not df.empty:
                        st.dataframe(df)
                        
                        # Enhanced Multi-Column Auto-Charting
                        chart_data = None
                        if len(df) > 1:
                            # 1. First column is the X-axis (Names/Labels)
                            x_col = df.columns[0]
                            
                            # 2. Try to convert other columns to numeric if they are strings/objects
                            # (Important because SUM() or Large Numbers often come as Decimal/Object)
                            for col in df.columns[1:]:
                                df[col] = pd.to_numeric(df[col], errors='ignore')
                            
                            # 3. Get all numeric columns
                            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                            
                            if len(numeric_cols) > 0:
                                # Ensure we have multiple bars if there are multiple stats
                                fig = px.bar(
                                    df, 
                                    x=x_col, 
                                    y=numeric_cols, 
                                    barmode='group',
                                    template="plotly_dark",
                                    title=f"Comparison: {', '.join(numeric_cols)} per {x_col}"
                                )
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Store for history
                                chart_data = (x_col, numeric_cols)
                
                st.session_state.messages.append({"role": "assistant", "content": final_answer, "data": df, "chart_data": chart_data})
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
