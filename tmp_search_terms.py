import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Searching for Antibiotics in Groups ---")
cur.execute("SELECT DISTINCT \"group\" FROM product_groups WHERE \"group\" ILIKE '%An%'")
print([r[0] for r in cur.fetchall()])

print("\n--- Searching for Karachi in Areas ---")
cur.execute("SELECT DISTINCT name FROM areas WHERE name ILIKE '%KHI%' OR name ILIKE '%Karachi%'")
print([r[0] for r in cur.fetchall()])

cur.close()
conn.close()
