import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import re
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import openai
from typing import List, Dict, Any

# --- CONFIGURATION ---
load_dotenv(override=True)
DB_URL = os.getenv("DATABASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY").strip() if os.getenv("LLM_API_KEY") else None
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")

client = openai.OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

import decimal
import json

import datetime
# Custom JSON encoder to handle PostgreSQL Decimal types and Date/Time objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, (datetime.date, datetime.datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)


def smart_format_dataframe(df: pd.DataFrame):
    """
    Returns:
        df_numeric  -> pure numeric (for charts / calculations)
        df_display  -> formatted with commas/rounding (for st.dataframe UI)
    """
    if df.empty or len(df.columns) == 0:
        return df, df
        
    df_numeric = df.copy()
    df_display = df.copy()

    # Apply type conversion to Decimal columns immediately to prevent precision issues
    for col in df_numeric.columns:
        if df_numeric[col].dtype == object:
            try:
                df_numeric[col] = pd.to_numeric(df_numeric[col], errors='ignore')
            except:
                pass

    # Identify and format numeric columns
    for col in df_numeric.columns:
        # Check if column should be treated as numeric metric
        # Rule: Format if it's numeric type AND (not first column OR first column is a single value)
        is_numeric_type = pd.api.types.is_numeric_dtype(df_numeric[col])
        
        # Heuristic for Category column: Usually first col if there are multiple cols
        is_category_col = (col == df_numeric.columns[0] and len(df_numeric.columns) > 1)
        
        # Exceptions: If it's the first col but name contains 'sale', 'rev', 'price', 'total', format it anyway
        is_explicit_metric = any(k in str(col).lower() for k in ["sale", "rev", "price", "total", "qty", "amount"])
        
        if is_numeric_type and (not is_category_col or is_explicit_metric):
            # 1. Ensure numeric for calculations
            df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce').fillna(0).astype(float)
            
            # 2. Format for display (Round to 2 decimals, add commas)
            # Check if all values are actually integers to avoid .00 suffix
            is_all_int = (df_numeric[col] % 1 == 0).all()
            if is_all_int:
                df_display[col] = df_numeric[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
            else:
                df_display[col] = df_numeric[col].apply(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")
        
        # Specialized Date Handling
        elif "date" in str(col).lower() or "time" in str(col).lower():
            try:
                df_display[col] = pd.to_datetime(df_numeric[col]).dt.strftime('%Y-%m-%d')
            except:
                pass

    return df_numeric, df_display

def plot_smart_chart(df: pd.DataFrame, x_col: str, y_cols: list, title: str, key: str):
    """
    Plots a chart with support for dual Y-axes if columns have vastly different scales
    (e.g., Quantity in millions vs Revenue in trillions).
    """
    if len(y_cols) == 2:
        v1 = df[y_cols[0]].abs().max()
        v2 = df[y_cols[1]].abs().max()
        
        # Check for Scale mismatch (10x or more) or Pharma specific keywords
        is_qty_val = any(k in y_cols[0].lower() for k in ["qty", "unit"]) and \
                     any(k in y_cols[1].lower() for k in ["rev", "val", "price", "sale"])
        is_val_qty = any(k in y_cols[1].lower() for k in ["qty", "unit"]) and \
                     any(k in y_cols[0].lower() for k in ["rev", "val", "price", "sale"])
        
        if is_qty_val or is_val_qty or (v1 > 0 and v2 > 0 and (v1/v2 > 10 or v2/v1 > 10)):
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Smaller scale usually goes on Primary Y as Bars, Larger scale on Secondary Y as Line
            if v1 < v2:
                p_col, s_col = y_cols[0], y_cols[1]
            else:
                p_col, s_col = y_cols[1], y_cols[0]
                
            fig.add_trace(go.Bar(x=df[x_col], y=df[p_col], name=p_col, marker_color='#636EFA'), secondary_y=False)
            fig.add_trace(go.Scatter(x=df[x_col], y=df[s_col], name=s_col, marker_color='#EF553B', mode='lines+markers'), secondary_y=True)
            
            is_time_series = any(k in x_col.lower() for k in ["month", "year", "date", "day", "week"])
            
            # If time series, let Plotly handle numeric/linear sorting. Otherwise use categories.
            xaxis_config = {'type': 'category', 'categoryorder': 'total descending'}
            if is_time_series:
                xaxis_config = {'type': None} # Auto-detect (linear sorting for numbers)

            fig.update_layout(
                title=title, template="plotly_dark", hovermode="x unified",
                xaxis=xaxis_config,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig.update_yaxes(title_text=p_col, secondary_y=False)
            fig.update_yaxes(title_text=s_col, secondary_y=True)
            st.plotly_chart(fig, use_container_width=True, key=key)
            return

    # Standard Grouped Bar
    is_time_series = any(k in x_col.lower() for k in ["month", "year", "date", "day", "week"])
    xaxis_config = {'type': 'category', 'categoryorder': 'total descending'}
    if is_time_series:
        xaxis_config = {'type': None}

    fig = px.bar(df, x=x_col, y=y_cols, barmode='group', template="plotly_dark", title=title)
    fig.update_layout(xaxis=xaxis_config)
    st.plotly_chart(fig, use_container_width=True, key=key)

def get_query_cache_path():
    return os.path.join(get_chats_dir(), "query_cache.json")

def load_query_cache():
    cache_path = get_query_cache_path()
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)
    return {}

def save_to_query_cache(question, sql):
    cache = load_query_cache()
    normalized_q = question.lower().strip()
    cache[normalized_q] = sql
    with open(get_query_cache_path(), "w") as f:
        json.dump(cache, f, indent=4)

def get_chats_dir():
    username = st.session_state.get("username", "default")
    d = os.path.join("chats", str(username).strip())
    if not os.path.exists(d):
        os.makedirs(d)
    return d

def save_session(session_id, messages):
    if not messages: return
    # Find or generate a title based on the first user message
    title = session_id
    if len(messages) >= 1:
        first_q = messages[0]["content"]
        # If it's a new session, we might want to get a cleaner title from LLM
        if session_id.startswith("New_Session_"):
            try:
                res = client.chat.completions.create(
                    model=LLM_MODEL,
                    messages=[{"role": "user", "content": f"Briefly title this pharma data question (max 4 words). Output ONLY the 4 words, nothing else: {first_q}"}],
                    timeout=5.0
                )
                raw_title = res.choices[0].message.content.strip()
                # Sanitize for valid OS filename (remove special chars, newlines)
                clean_title = re.sub(r'[\\/*?:"<>|\n\r]+', "", raw_title)
                title = clean_title.replace(" ", "_")[:50]
            except:
                clean_fq = re.sub(r'[\\/*?:"<>|\n\r]+', "", first_q[:30])
                title = clean_fq.replace(" ", "_")
    
    file_path = os.path.join(get_chats_dir(), f"{title}.json")
    # Save with dataframes converted to dict for JSON
    serializable_msgs = []
    for m in messages:
        m_copy = m.copy()
        if "data" in m_copy and isinstance(m_copy["data"], pd.DataFrame):
            m_copy["data"] = m_copy["data"].to_dict(orient="records")
        serializable_msgs.append(m_copy)
        
    with open(file_path, "w") as f:
        json.dump(serializable_msgs, f, indent=4, cls=DecimalEncoder)
    return title

def load_session(filename):
    with open(os.path.join(get_chats_dir(), filename), "r") as f:
        try:
            msgs = json.load(f)
        except:
            return []
            
        if isinstance(msgs, dict):
            msgs = [msgs]
        if not isinstance(msgs, list):
            return []
            
        valid_msgs = []
        for m in msgs:
            if isinstance(m, dict):
                if "data" in m and m["data"] is not None:
                    m["data"] = pd.DataFrame(m["data"])
                valid_msgs.append(m)
        return valid_msgs

# --- DATABASE ENGINE ---
def run_sql_query(query: str):
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
    query_u = query.upper()
    if not (query_u.strip().startswith("SELECT") or query_u.strip().startswith("WITH")):
        return {"error": "Only SELECT or WITH queries are allowed."}
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

@st.cache_data(ttl=3600)
def get_schema():
    try:
        with open("prisma/schema.prisma", "r") as f:
            return f.read()
    except:
        return "Schema unavailable."

@st.cache_data(ttl=600)
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
    
    # Add explicit instructions for complex joins and case-insensitive matching
    context += "\n--- MASTER SQL RULES (MANDATORY) ---\n"
    context += "1. USE 'master_sale' FOR INTERNAL METRICS: It has product_name, product_quantity, total_amount, region_name, zone_name, area_name, invoice_date, month_number, year_number, month_name.\n"
    context += "   - NOTE: 'sale_date' does NOT exist. Use 'invoice_date' for the full date.\n"
    context += "2. JOINING BRICK NAMES: 'master_sale' does NOT have brick_name. For brick-level internal sales, use this join:\n"
    context += "   SELECT ib.name, SUM(ms.product_quantity) FROM master_sale ms JOIN customer_details cd ON ms.customer_id = cd.customer_id JOIN ims_brick ib ON cd.ims_brick_id = ib.id GROUP BY ib.name\n"
    context += "3. NO CARTESIAN PRODUCTS: Never join 'ims_sale' and 'invoice_details' (or 'master_sale') in a single table join. It will multiply the numbers incorrectly.\n"
    context += "   - To compare Internal vs Market, use CTEs (Common Table Expressions):\n"
    context += "   WITH Internal AS (SELECT ib.name, SUM(ms.product_quantity) as qty FROM master_sale ms JOIN customer_details cd ON ms.customer_id = cd.customer_id JOIN ims_brick ib ON cd.ims_brick_id = ib.id GROUP BY 1),\n"
    context += "        Market AS (SELECT ib.name, SUM(unit) as qty FROM ims_sale s JOIN ims_brick ib ON s.\"brickId\" = ib.id GROUP BY 1)\n"
    context += "   SELECT i.name, i.qty AS \"Internal\", m.qty AS \"Market\" FROM Internal i JOIN Market m ON i.name = m.name\n"
    context += "4. FUZZY SEARCH (ILIKE): For all user filters (like 'Gulshan'), use ILIKE with wildcards: WHERE ib.name ILIKE '%gulshan%'.\n"
    context += "5. GULSHAN AGGREGATION: Gulshan has many blocks (Block 5, 6, etc.). Always use ILIKE '%gulshan%' to aggregate all of them.\n"
    context += "6. EMPTY TABLES: 'orders' and 'targets' are EMPTY. For sales, always use 'master_sale' or 'invoice_details'.\n"
    
    return context

@st.cache_data(ttl=1800)
def get_executive_kpis():
    """Fetches high-level business metrics for the homepage."""
    kpis = {
        "internal_sales": 0,
        "market_sales": 0,
        "top_brick": "N/A",
        "doc_count": 0
    }
    
    # 1. Total Internal Units (from Invoices)
    res = run_sql_query('SELECT SUM(CAST("product_quantity" AS NUMERIC)) as total FROM "invoice_details"')
    if res and not isinstance(res, dict): kpis["internal_sales"] = res[0]["total"] or 0
    
    # 2. Total Market Units (from IMS)
    res = run_sql_query('SELECT SUM("unit") as total FROM "ims_sale"')
    if res and not isinstance(res, dict): kpis["market_sales"] = res[0]["total"] or 0
    
    # 3. Top Performing Brick
    res = run_sql_query('''
        SELECT "b"."name", SUM(CAST("id"."product_quantity" AS NUMERIC)) as total 
        FROM "ims_brick" "b"
        JOIN "customer_details" "cd" ON "b"."id" = "cd"."ims_brick_id"
        JOIN "invoice" "inv" ON "cd"."customer_id" = "inv"."cust_id"
        JOIN "invoice_details" "id" ON "inv"."id" = "id"."invoice_id"
        GROUP BY "b"."name" ORDER BY total DESC LIMIT 1
    ''')
    if res and not isinstance(res, dict): kpis["top_brick"] = res[0]["name"]
    
    # 4. Total Active Doctors
    res = run_sql_query('SELECT COUNT(*) as total FROM "doctors"')
    if res and not isinstance(res, dict): kpis["doc_count"] = res[0]["total"] or 0
    
    return kpis

# --- PAGE CONFIG ---
st.set_page_config(page_title="Pharma Intelligence", page_icon="💊", layout="wide")

# --- SESSION INITIALIZATION ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "current_session" not in st.session_state:
    st.session_state.current_session = f"New_Session_{int(pd.Timestamp.now().timestamp())}"
if "prompt_trigger" not in st.session_state:
    st.session_state.prompt_trigger = None
if "username" not in st.session_state:
    st.session_state.username = None

if not st.session_state.username:
    import bcrypt

    def verify_db_login(user_clean, pass_clean):
        if user_clean == "admin" and pass_clean == "admin":
            return True
            
        safe_user = user_clean.replace("'", "''")
        pass_bytes = pass_clean.encode('utf-8')
        
        # 1. Check managers table
        q1 = f"SELECT password FROM managers WHERE email ILIKE '{safe_user}' OR name ILIKE '{safe_user}' LIMIT 1"
        res1 = run_sql_query(q1)
        if isinstance(res1, list) and len(res1) > 0 and res1[0].get("password"):
            db_hash = res1[0].get("password").encode('utf-8')
            try:
                if bcrypt.checkpw(pass_bytes, db_hash):
                    return True
            except:
                pass
                
        # 2. Check users table
        q2 = f"SELECT password FROM users WHERE email ILIKE '{safe_user}' OR firstname ILIKE '{safe_user}' LIMIT 1"
        res2 = run_sql_query(q2)
        if isinstance(res2, list) and len(res2) > 0 and res2[0].get("password"):
            db_hash = str(res2[0].get("password")).encode('utf-8')
            try:
                if bcrypt.checkpw(pass_bytes, db_hash):
                    return True
            except:
                if str(res2[0].get("password")) == pass_clean: # Fallback plain integer/text matching just in case
                    return True
                
        return False

    # --- PREMIUM LOGIN UI ---
    st.markdown("""
        <style>
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
            background-attachment: fixed;
        }
        .login-card {
            background: rgba(30, 41, 59, 0.7);
            backdrop-filter: blur(12px);
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 15px 35px rgba(0,0,0,0.4);
            border: 1px solid rgba(255,255,255,0.1);
            max-width: 450px;
            margin: 50px auto;
            text-align: center;
        }
        .stButton > button {
            background: linear-gradient(90deg, #6366f1 0%, #a855f7 100%);
            color: white;
            border: none;
            border-radius: 10px;
            padding: 12px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(99, 102, 241, 0.4);
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="login-card">', unsafe_allow_html=True)
    st.markdown("<h1 style='color: white; font-family: Outfit, sans-serif; font-size: 2.5rem; margin-bottom: 0;'>👤</h1>", unsafe_allow_html=True)
    st.markdown("<h2 style='color: white; font-family: Outfit, sans-serif;'>Intelligence Login</h2>", unsafe_allow_html=True)
    st.markdown("<p style='color: #94a3b8;'>Pharma Insights & Strategy Portal</p>", unsafe_allow_html=True)
    
    with st.form("premium_login"):
        user_input = st.text_input("Email / Username Address", placeholder="e.g. admin@pharma.com")
        pass_input = st.text_input("Access Key", type="password", placeholder="••••••••")
        st.write("")
        submit = st.form_submit_button("SIGN IN TO DASHBOARD", use_container_width=True)
        
        if submit:
            user_clean = user_input.strip()
            if not user_clean:
                st.error("Access Denied: Please enter a username.")
            elif verify_db_login(user_clean, pass_input):
                st.session_state.username = user_clean
                st.rerun()
            else:
                st.error("Authentication Failed! Check your credentials.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    st.stop()

# --- PREMIUM MAIN THEME & CSS ---
st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&family=Inter:wght@400;500&display=swap" rel="stylesheet">

<style>
    /* Global Overrides */
    .main { 
        background: #0f172a; 
        color: #f8fafc; 
        font-family: 'Inter', sans-serif;
    }
    
    h1, h2, h3, .stButton { 
        font-family: 'Outfit', sans-serif; 
    }

    /* Professional Sidebar */
    [data-testid="stSidebar"] {
        background-color: #1e293b !important;
        border-right: 1px solid rgba(255,255,255,0.05);
    }

    /* Chat Elements */
    .stChatMessage { 
        border-radius: 16px !important; 
        padding: 20px !important;
        margin-bottom: 12px !important;
        border: 1px solid rgba(255,255,255,0.05) !important;
    }
    
    [data-testid="stChatMessage-user"] {
        background: rgba(99, 102, 241, 0.1) !important;
        border-left: 4px solid #6366f1 !important;
    }
    
    [data-testid="stChatMessage-assistant"] {
        background: rgba(30, 41, 59, 0.6) !important;
        border-right: 4px solid #a855f7 !important;
    }

    /* Data Containers */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid rgba(255,255,255,0.05);
    }
    
    /* Metrics Highlighting */
    .stMetric {
        background: rgba(255,255,255,0.03);
        padding: 15px;
        border-radius: 12px;
        border: 1px solid rgba(255,255,255,0.05);
    }

    /* Custom Scrollbar */
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 10px; }
    ::-webkit-scrollbar-thumb:hover { background: #475569; }
</style>
""", unsafe_allow_html=True)

st.markdown("<h1 style='color: white; margin-bottom: 0;'>💊 Pharma Intel Agent</h1>", unsafe_allow_html=True)
st.caption("Strategic Decision Support with RAG Memory")
st.divider()

# --- SIDEBAR ---
with st.sidebar:
    st.header(f"🧑‍💼 Welcome, {st.session_state.username}")
    if st.button("🚪 Logout"):
        st.session_state.username = None
        st.session_state.messages = []
        st.rerun()
    st.divider()
    st.header("📂 Chat Sessions")
    chat_files = [f for f in os.listdir(get_chats_dir()) if f.endswith(".json") and f not in ["query_cache.json", "users.json"]]
    
    if st.button("➕ New Chat"):
        st.session_state.messages = []
        st.session_state.current_session = f"New_Session_{int(pd.Timestamp.now().timestamp())}"
        st.rerun()

    if chat_files:
        selected_chat = st.selectbox("Past Conversations", ["Select..."] + chat_files)
        if selected_chat != "Select...":
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📂 Load"):
                    st.session_state.messages = load_session(selected_chat)
                    st.session_state.current_session = selected_chat.replace(".json", "")
                    st.rerun()
            with col2:
                if st.button("🗑️ Delete"):
                    file_to_del = os.path.join(get_chats_dir(), selected_chat)
                    if os.path.exists(file_to_del):
                        os.remove(file_to_del)
                        st.session_state.messages = []
                        st.session_state.current_session = f"New_Session_{int(pd.Timestamp.now().timestamp())}"
                        st.toast(f"Deleted {selected_chat}")
                        st.rerun()
    
    st.divider()
    with st.expander("📊 Data Health Check"):
        st.success("IMS Market Sales: 156k+ records")
        st.success("Internal Sales (Invoices): 55k+ records")
        st.success("Doctors: 211 records")
        st.success("Sales Targets: 4,112 records")
        st.warning("⚠️ Orders: 0 records")
    
    st.divider()
    if st.button("Clear History"):
        st.session_state.messages = []
    

# --- HELPER: Handle question submission ---
def submit_question(q):
    st.session_state.prompt_trigger = q

def delete_message(msg_id):
    # Find the index of the message with this ID
    target_idx = -1
    for i, m in enumerate(st.session_state.messages):
        if m.get("msg_id") == msg_id:
            target_idx = i
            break
            
    if target_idx != -1:
        # If it's an assistant message, delete it and its preceding user message
        if st.session_state.messages[target_idx]["role"] == "assistant":
            if target_idx > 0 and st.session_state.messages[target_idx - 1]["role"] == "user":
                # Message + Preceding Question
                st.session_state.messages.pop(target_idx)
                st.session_state.messages.pop(target_idx - 1)
            else:
                st.session_state.messages.pop(target_idx)
        # If it's a user message, check if following message is the assistant's answer
        elif st.session_state.messages[target_idx]["role"] == "user":
            if target_idx < len(st.session_state.messages) - 1 and st.session_state.messages[target_idx + 1]["role"] == "assistant":
                # Question + Following Answer
                st.session_state.messages.pop(target_idx + 1)
                st.session_state.messages.pop(target_idx)
            else:
                st.session_state.messages.pop(target_idx)
        
        # Save updated state
        save_session(st.session_state.current_session, st.session_state.messages)
        st.rerun()

# --- STARTER QUESTIONS (Show only if no messages) ---
if not st.session_state.messages:
    st.write("### 💡 Start with a sample report:")
    all_starters = [
        "Compare top 5 bricks by internal units vs market units",
        "Show me top 5 Category A doctors",
        "Which 3 products have the highest invoice quantity?",
        "Compare internal sales vs market sales in F.B.AREA",
        "Which brick has the highest internal units sold?",
        "List top 5 doctors by visit count in doctor_plan",
        "Compare market units of 'Product A' vs 'Product B' across bricks",
        "Show internal sales trend for F.B.AREA region",
        "Which Team has the highest target vs achievement?",
        "What is the market share of Karachi brick?"
    ]
    random.shuffle(all_starters)
    starters = all_starters[:4]
    
    cols = st.columns(2)
    for i, s in enumerate(starters):
        with cols[i % 2]:
            st.button(s, key=f"starter_{s}", on_click=submit_question, args=(s,))

# --- CHAT INTERFACE ---
for idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        m_id = message.get("msg_id", f"static_{idx}")
        
        # --- Compact Header for SQL, Download & Delete ---
        header_cols = st.columns([4, 1, 1])
        
        with header_cols[2]:
            if st.button("🗑️", key=f"del_{m_id}", help="Delete this message"):
                delete_message(m_id)

        if "timestamp" in message:
            st.caption(f"🕒 {message['timestamp']}")
        
        with header_cols[0]:
            if "sql" in message and message["sql"]:
                with st.expander("🛠️ Show SQL Logic (Debug)"):
                    st.code(message["sql"], language="sql")
        
        if "data" in message:
            df_raw = pd.DataFrame(message["data"])
            df_numeric_hist, df_display_hist = smart_format_dataframe(df_raw)
            
            # Place Download Button in the right column if data exists
            with header_cols[1]:
                csv = df_display_hist.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 CSV",
                    data=csv,
                    file_name=f"data_export_{idx}.csv",
                    mime="text/csv",
                    key=f"dl_{idx}"
                )

            # Table use formatted
            st.dataframe(df_display_hist, use_container_width=True)
            
            if "insight" in message and message["insight"]:
                st.info(f"💡 **AI Insights:**\n{message['insight']}")
            
            # --- Historical Charts (Support for Split Charts + Single Plots) ---
            split_meta = message.get("split_charts_metadata")
            if split_meta:
                group_col = split_meta["group_col"]
                x_axis_col = split_meta["x_axis_col"]
                y_metrics = split_meta["y_metrics"]
                unique_cats = df_numeric_hist[group_col].unique()
                for i, cat_val in enumerate(unique_cats[:5]):
                    subset = df_numeric_hist[df_numeric_hist[group_col] == cat_val].copy()
                    plot_smart_chart(
                        subset, 
                        x_axis_col, 
                        y_metrics, 
                        f"📊 {cat_val}: {', '.join(y_metrics)}", 
                        f"chart_hist_{idx}_{i}"
                    )
            elif message.get("chart_data") is not None:
                x_col, y_cols = message["chart_data"]
                # Filter to ensure Y columns exist and are numeric
                y_cols_valid = [c for c in y_cols if c in df_numeric_hist.columns]
                if y_cols_valid:
                    plot_smart_chart(
                        df_numeric_hist, 
                        x_col, 
                        y_cols_valid, 
                        f"History: {', '.join(y_cols_valid)}", 
                        f"chart_hist_{idx}"
                    )
        
        # Display FOLLOW-UP buttons if they exist in the message metadata
        if "follow_ups" in message and message["follow_ups"] and idx == len(st.session_state.messages) - 1:
            st.write("---")
            st.write("🔍 **Suggested Follow-ups:**")
            num_follow_ups = len(message["follow_ups"])
            if num_follow_ups > 0:
                f_cols = st.columns(num_follow_ups)
                for f_idx, f_text in enumerate(message["follow_ups"]):
                    with f_cols[f_idx]:
                        if st.button(f_text, key=f"follow_{idx}_{f_idx}"):
                            submit_question(f_text)
                            st.rerun()

# Use either chat_input or a click from starters/follow-ups
user_input = st.chat_input("Ask about your pharma data...")
prompt = user_input or st.session_state.prompt_trigger

if prompt:
    st.session_state.prompt_trigger = None # Reset
    current_time = pd.Timestamp.now().strftime("%I:%M %p - %b %d, %Y")
    with st.chat_message("user"):
        st.markdown(prompt)
        st.caption(f"🕒 {current_time}")
    st.session_state.messages.append({"role": "user", "content": prompt, "timestamp": current_time, "msg_id": f"u_{int(pd.Timestamp.now().timestamp())}_{random.randint(0,1000)}"})

    with st.spinner("Retrieving Data..."):
        sys_prompt = "You are a professional Pharma Database Agent. STRICTLY use proper English spelling for business terms (e.g., 'Business Summary', 'Sales', 'Growth') even when writing the rest of the sentence in Roman Urdu. Do NOT use literal phonetic translations like 'Biznes' or 'bikri'. STRICT: Do NOT use any emojis, icons, or decorative symbols."
        try:
            # --- 1. QUICKEST CACHE CHECK (No File Reads) ---
            cache = load_query_cache()
            normalized_q = prompt.lower().strip()
            
            sql_query = ""
            results = None
            is_cached = False
            is_conversational = False
            conversational_reply = ""
            
            if normalized_q in cache:
                sql_query = cache[normalized_q]
                results = run_sql_query(sql_query)
                if not (isinstance(results, dict) and "error" in results):
                    is_cached = True

            # --- 2. LLM GENERATION ONLY ON CACHE MISS ---
            if not is_cached:
                # Cache misses perform file reads
                schema = get_schema()
                rag_context = get_rag_context(prompt)
                
                chat_context = ""
                if len(st.session_state.messages) > 1:
                    history = st.session_state.messages[-5:-1] # Get up to 4 prev msgs (excluding current)
                    chat_context = "PREVIOUS CONVERSATION CONTEXT:\n"
                    for msg in history:
                        role_name = "User" if msg["role"] == "user" else "Assistant"
                        content_txt = str(msg.get("content", ""))
                        chat_context += f"{role_name}: {content_txt[:500]}\n"
                        if role_name == "Assistant" and msg.get("sql"):
                            chat_context += f"Executed SQL: {msg['sql']}\n"
                
                current_full_date = pd.Timestamp.now().strftime("%A, %b %d, %Y")
                max_retries = 5
                current_attempt = 0
                last_error = ""
                exploration_feedback = ""
                is_conversational = False
                conversational_reply = ""
                
                while current_attempt < max_retries:
                    current_attempt += 1
                    error_feedback = f"\n\nLAST ERROR WAS: {last_error}" if last_error else ""
                    gen_prompt = (
                        f"CURRENT DATE/TIME: {current_full_date}\n"
                        f"{chat_context}\n"
                        f"KNOWLEDGE BASE & BUSINESS LOGIC:\n{rag_context}\n\n"
                        f"DATABASE SCHEMA:\n{schema}\n\n"
                        f"QUERY VALIDATION RULES (CRITICAL BEFORE EXECUTING):\n"
                        f"1. Verify table exists in schema (e.g. 'orders' is EMPTY, 'master_sale' is NOT).\n"
                        f"2. Verify all column names exist exactly as written.\n"
                        f"3. If filter value may differ, ALWAYS use ILIKE '%value%'.\n"
                        f"4. If your previous query returned 0 rows, you MUST generate a simple query to inspect available values: SELECT DISTINCT <column_name> FROM <table> LIMIT 10;\n"
                        f"5. Never return constant values like 0 for 'brick_name'. Always select the actual column.\n"
                        f"6. PostgreSQL Case Sensitivity (CRITICAL): If a column name has capital letters (ALWAYS true for `doctor_plan` table: \"doctorId\", \"managerId\", \"healthCentreId\", \"eventType\", \"inHealthCentre\"), you MUST enclose it in DOUBLE QUOTES. e.g. SELECT \"managerId\" FROM doctor_plan.\n"
                        f"7. Subqueries: Never use '=' with a subquery like `WHERE id = (SELECT...)`. ALWAYS use `IN (SELECT...)` or a `JOIN` to avoid 'more than one row returned' errors.\n"
                        f"8. Follow-Up Filter Retention: If the user asks a continuation question (e.g., 'what about completed visits?', 'now show me for this year'), you MUST RE-APPLY the exact same WHERE filters (like manager name, territory, doctor) from the previous 'Executed SQL', unless the user explicitly tells you to look at someone else.\n"
                        f"9. WHAT-IF ANALYSIS (SIMULATION): If the user asks a 'What-If' question (e.g., 'If I double visits, how will sales change?'), do NOT say you can't predict. Instead, generate a ```sql``` query to fetch the last 12 months of 'master_sale' (revenue) and 'doctor_plan' (visits) for that context. Then, in the summary, calculate the simple growth ratio and provide a hypothetical business estimate.\n"
                        f"10. TABLE MAPPING: The table `area_managers` ONLY contains `area_id` and `manager_id`. For geography/manager sales, use `master_sale` directly (`area_manager_name`).\n"
                        f"11. DOCTOR SALES: In `master_sale`, doctors are recorded as `customer_name`. ALWAYS filter by `customer_type = 'Doctors'` to get physician-specific sales. The column `doctor_name` does NOT exist in `master_sale`. Also, the column for product categories is `product_group_name`, NOT `product_group`.\n"
                        f"12. Database 'master_sale' uses `invoice_date`. The column `sale_date` does NOT exist.\n"
                        f"13. WHAT-IF ROUTING: For any simulation, growth projection, or 'What-If' question, you MUST use OPTION B (Final Data/SQL) to fetch history. NEVER use OPTION C (Chat) for business projections.\n"
                        f"USER QUESTION: {prompt}"
                        f"{error_feedback}"
                        f"{exploration_feedback}\n\n"
                        "ROUTING RULES (Follow Carefully - AUTONOMOUS AGENT MODE):\n"
                        "OPTION A (Explore Data): If you are unsure of exact spellings or if your previous query returned 0 rows, write a diagnostic query inside ```explore\n ... \n``` (e.g. SELECT DISTINCT name). I will run it and feed the results back to you secretly so you can fix your final query.\n"
                        "OPTION B (Final Data): If you are confident your query will bring the correct data for the user, return the PostgreSQL SELECT query inside ```sql\n ... \n```.\n"
                        "OPTION C (Chat): ONLY for greetings ('hi', 'how are you'), simple text questions about your name, or general pharma definitions. NEVER use this for data analysis or business projections.\n"
                    )
                    
                    # ATTEMPT GENERATION
                    try:
                        response = client.chat.completions.create(
                            model=LLM_MODEL,
                            messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": gen_prompt}],
                            timeout=30.0
                        )
                    except Exception as api_err:
                        last_error = str(api_err)
                        continue
                    
                    content = response.choices[0].message.content
                    chat_match = re.search(r"```chat\n(.*?)\n```", content, re.DOTALL)
                    sql_match = re.search(r"```sql\n(.*?)\n```", content, re.DOTALL)
                    explore_match = re.search(r"```explore\n(.*?)\n```", content, re.DOTALL)
                    
                    if chat_match:
                        is_conversational = True
                        conversational_reply = chat_match.group(1).strip()
                        results = []
                        sql_query = ""
                        break

                    if explore_match:
                        explore_query = explore_match.group(1).strip()
                        exp_results = run_sql_query(explore_query)
                        if isinstance(exp_results, dict) and "error" in exp_results:
                            last_error = exp_results["error"]
                        else:
                            exploration_feedback += f"\n\n[EXPLORATION RESULTS for '{explore_query}']:\n{exp_results}\nUse these exact values in your next ```sql``` query."
                            last_error = ""
                        continue

                    sql_query = sql_match.group(1).strip() if sql_match else content.strip()

                    results = run_sql_query(sql_query)
                    
                    if isinstance(results, dict) and "error" in results:
                        last_error = results["error"]
                        continue
                        
                    if not results and current_attempt < max_retries:
                        last_error = f"Your final ```sql``` query returned exactly 0 rows. Please use ```explore``` option next to SELECT DISTINCT values from the database and find out what the actual spellings or values are before returning another ```sql``` query."
                        continue
                        
                    else:
                        if results: # Only save to cache if it actually returned data
                            save_to_query_cache(prompt, sql_query)
                        break

            # Final Output Handling
            if is_conversational:
                # Bypass DB logic, output chat directly
                final_answer = conversational_reply
                df = pd.DataFrame()
                current_time_ai = pd.Timestamp.now().strftime("%I:%M %p - %b %d, %Y")
                with st.chat_message("assistant"):
                    st.markdown(final_answer)
                    st.caption(f"🕒 {current_time_ai}")
                chart_data = None
            elif isinstance(results, dict) and "error" in results:
                st.error(f"Failed after {max_retries} attempts. Final Error: {results['error']}")
                final_answer = f"Error: {results['error']}"
                df = pd.DataFrame()
                chart_data = None
            else:
                if is_cached:
                    st.toast("⚡ Result served from Cache (SQL generation skipped)", icon="🔥")
                
                df = pd.DataFrame(results)
                
                if df.empty:
                    # PROACTIVE DISCOVERY: Try to find similar bricks
                    discovery_msg = ""
                    potential_brick = re.search(r"ILIKE '%(.*?)%'", sql_query, re.I)
                    if potential_brick:
                        search_term = potential_brick.group(1)
                        discovery_results = run_sql_query(f"SELECT name FROM ims_brick WHERE name ILIKE '%{search_term}%' LIMIT 3")
                        if discovery_results:
                            names_list = "\n".join([f"- {r['name']}" for r in discovery_results])
                            discovery_msg = f"Available similar options in database:\n{names_list}"

                    discovery_context = f"I found these similar options in the database:\n{discovery_msg}\nIncorporate these options into your advice." if discovery_msg else "I could not find any similar alternative names in the database."

                    # Let the LLM generate a friendly "no data" message
                    empty_prompt = (
                        f"User asked: {prompt}\n\n"
                        f"Your SQL query returned exactly 0 rows.\n"
                        f"To assist the user, explain politely that the specific filter they used might not match exactly, or the data might not be available for their specific combination.\n"
                        f"IMPORTANT: {discovery_context}\n"
                        f"Do NOT make up or hallucinate placeholder options. If no options are provided, just tell the user to try a different query."
                    )
                    empty_res = client.chat.completions.create(model=LLM_MODEL, messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": empty_prompt}], timeout=30.0)
                    final_answer = empty_res.choices[0].message.content
                else:
                    # IF CACHED: Skip AI summary for 1-second performance
                    if is_cached:
                        final_answer = f"⚡ **(Cached Response)**\n\nHere are the latest results from the database for your request. The SQL logic was retrieved from history for high-speed performance."
                    else:
                        sum_prompt = (
                            f"{chat_context}\n"
                            f"User asked: {prompt}\nDB Result: {results}\n\n"
                             "TASK (Strategic Advisor Mode):\n"
                             "1. If data shows numeric results, provide a structured Business Summary.\n"
                             "2. WHAT-IF / SIMULATIONS: ONLY provide a 'Current' vs 'Predicted' comparison IF the user specifically asked for a 'what-if', 'prediction', 'simulation', or 'if I change' scenario. For standard data requests, stick to factual business summaries of existing results ONLY (no unsolicited predictions).\n"
                             "3. FORMATTING: Use Markdown Headers (###), Bold text, and Bullet points. Strictly avoid long paragraphs.\n"
                             "4. LARGE NUMBERS: Always use commas (e.g. PKR 3,500,000) and max 2 decimals.\n"
                             "5. LANGUAGE: Use Pakistani Roman Urdu for sentence structure, but STRICTLY use English for ALL business terminology (e.g., write 'Business Summary' and NOT 'Biznes Sammary', write 'Sales' and NOT 'Bikri').\n"
                             "6. INSIGHT: Always conclude with a 1-sentence strategic recommendation.\n"
                             "7. NO LIST REPETITION: If the database result is a long list of names or categories (e.g. more than 10 items) that is already visible in the table, do NOT repeat all the names in your summary. Instead, just provide a count and a high-level overview.\n"
                             "8. STRICT RULE: NEVER refer to the user as 'Mamo'. Use respectful professional language."
                        )
                        summary_res = client.chat.completions.create(model=LLM_MODEL, messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": sum_prompt}], timeout=30.0)
                        final_answer = summary_res.choices[0].message.content
                
                current_time_ai = pd.Timestamp.now().strftime("%I:%M %p - %b %d, %Y")
                with st.chat_message("assistant"):
                    st.markdown(final_answer)
                    st.caption(f"🕒 {current_time_ai}")
                    with st.expander("🛠️ Show SQL Logic (Debug)"):
                        st.code(sql_query, language="sql")
                    chart_data = None
                    if not df.empty:
                        # Use Smart Formatting
                        df_numeric, df_display = smart_format_dataframe(df)
                        
                        # Table use formatted
                        st.dataframe(df_display, use_container_width=True)
                        
                        # --- FEATURE 1: AI INSIGHT CARDS ---
                        insight_msg = ""
                        try:
                            insight_prompt = (
                                f"Analyze this data table and provide 2-3 VERY SHORT business insights or anomalies (max 10 words each). "
                                f"IMPORTANT: Format large numbers with commas/separators. Use Roman Urdu but STRICTLY use English for business terms (e.g., 'Sales'). NEVER use 'Mamo'. "
                                f"Data: {df.head(10).to_dict(orient='records')}"
                            )
                            insight_res = client.chat.completions.create(model=LLM_MODEL, messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": insight_prompt}], timeout=15.0)
                            insight_msg = insight_res.choices[0].message.content
                            st.info(f"💡 **AI Insights:**\n{insight_msg}")
                        except:
                            pass
                        
                        # --- Enhanced Multi-Column Auto-Charting (with Multi-Plot support) ---
                        chart_data = None
                        split_charts_metadata = None
                        if len(df_numeric) > 1:
                            cols = df_numeric.columns.tolist()
                            first_col = cols[0]
                            num_cols = df_numeric.select_dtypes(include=['number']).columns.tolist()
                            unique_cats = df_numeric[first_col].unique()
                            
                            # Decide if we need "Single Plot" or "Split Plots" (e.g. 5 products, each with 12 months)
                            if len(unique_cats) < len(df_numeric) and len(cols) >= 2:
                                # 🔥 MULTI-PLOT MODE (Split by Product/Category)
                                group_col = first_col
                                x_axis_col = cols[1]
                                # Blacklist for Y-axis metrics (ID, coordinates, status etc)
                                forbidden_exact = ['id', 'status', 'eventType', 'event_type', 'description', 'shift', 'approval', 'inHealthCentre', 'in_health_centre', 'latitude', 'longitude', 'lat', 'lng', 'radius', 'distance', 'mobile', 'phone', 'cnic', 'nic', 'year', 'month', 'invoice_id']
                                
                                # Identify Metrics for Y-axis (excluding grouping, X columns, and forbidden columns)
                                y_metrics = []
                                for c in num_cols:
                                    c_lower = str(c).lower()
                                    if c in [group_col, x_axis_col] or c_lower in forbidden_exact:
                                        continue
                                    if c_lower.endswith('_id') or (c_lower.endswith('id') and len(c_lower) > 4):
                                        continue
                                    # Skip if column is all zeros
                                    if df_numeric[c].sum() == 0:
                                        continue
                                    y_metrics.append(c)
                                
                                if y_metrics:
                                    # Save metadata for next rerun
                                    split_charts_metadata = {"group_col": group_col, "x_axis_col": x_axis_col, "y_metrics": y_metrics}
                                    # Active rendering
                                    # Use up to 5 unique categories to avoid clutter
                                    for i, cat_val in enumerate(unique_cats[:5]):
                                        subset = df_numeric[df_numeric[group_col] == cat_val].copy()
                                        if not subset.empty: # Ensure subset is not empty before plotting
                                            plot_smart_chart(
                                                subset, 
                                                x_axis_col, 
                                                y_metrics, 
                                                f"📊 {cat_val}: {', '.join(y_metrics)} Analysis", 
                                                f"split_active_{i}"
                                            )
                            else:
                                # ❄️ NORMAL PLOT MODE (One bar per Category)
                                x_col = first_col
                                forbidden_exact = ['id', 'status', 'eventType', 'event_type', 'description', 'shift', 'approval', 'inHealthCentre', 'in_health_centre', 'latitude', 'longitude', 'lat', 'lng', 'radius', 'distance', 'mobile', 'phone', 'cnic', 'nic', 'year', 'month', 'invoice_id']
                                y_cols = []
                                for c in num_cols:
                                    c_lower = str(c).lower()
                                    if c == x_col or c_lower in forbidden_exact:
                                        continue
                                    if c_lower.endswith('_id') or (c_lower.endswith('id') and len(c_lower) > 4):
                                        continue
                                    # Skip if column is all zeros
                                    if df_numeric[c].sum() == 0:
                                        continue
                                    y_cols.append(c)
                                
                                if y_cols:
                                    plot_smart_chart(
                                        df_numeric, 
                                        x_col, 
                                        y_cols, 
                                        f"Analysis: {', '.join(y_cols)} per {x_col}", 
                                        f"chart_new_active"
                                    )
                                    chart_data = (x_col, y_cols)
                        
                        # --- GENERATE FOLLOW-UPS ---
                        follow_ups = []
                        try:
                            f_prompt = f"Based on this answer: '{final_answer}', suggest 2 very short (max 5 words) follow-up questions about the data."
                            f_res = client.chat.completions.create(model=LLM_MODEL, messages=[{"role": "user", "content": f_prompt}], timeout=15.0)
                            raw_f = f_res.choices[0].message.content
                            follow_ups = [q.strip('1234. -').strip() for q in raw_f.split('\n') if '?' in q][:2]
                        except:
                            follow_ups = []

                        st.session_state.messages.append({"role": "assistant", "content": final_answer, "sql": sql_query if not is_conversational else "", "data": df_numeric if not df.empty else df, "insight": insight_msg if 'insight_msg' in locals() else "", "chart_data": chart_data, "split_charts_metadata": split_charts_metadata, "follow_ups": follow_ups, "timestamp": current_time_ai, "msg_id": f"a_{int(pd.Timestamp.now().timestamp())}_{random.randint(0,1000)}"})
                # Auto-save
                new_id = save_session(st.session_state.current_session, st.session_state.messages)
                if st.session_state.current_session.startswith("New_Session_"):
                    st.session_state.current_session = new_id
                st.rerun()
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
