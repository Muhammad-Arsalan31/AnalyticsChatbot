import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
clean_url = DB_URL.split('?')[0] if DB_URL else ""

try:
    conn = psycopg2.connect(clean_url)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cur.fetchall()
    print("Tables found in public schema:")
    for t in tables:
        print(f"- {t[0]}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
