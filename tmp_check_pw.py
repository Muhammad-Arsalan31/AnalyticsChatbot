import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
# Keep full url to include schema if any, although psycopg2 sometimes ignores it.
# We explicitly set search_path or use schema directly.
conn = psycopg2.connect(os.getenv('DATABASE_URL').split('?')[0])
cur = conn.cursor()

try:
    cur.execute("SELECT name, email, password FROM managers WHERE password IS NOT NULL LIMIT 5;")
    res = cur.fetchall()
    print("Found managers:", res)
except Exception as e:
    print("Error:", e)

conn.close()
