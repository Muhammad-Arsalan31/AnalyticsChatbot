import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Real Area Names (Sample) ---")
cur.execute("SELECT DISTINCT name FROM areas LIMIT 20")
print([r[0] for r in cur.fetchall()])

print("\n--- Real Product Groups (Sample) ---")
cur.execute("SELECT DISTINCT \"group\" FROM product_groups LIMIT 20")
print([r[0] for r in cur.fetchall()])

cur.close()
conn.close()
