import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Samples from master_sale ---")
cur.execute("SELECT DISTINCT product_group_name FROM master_sale LIMIT 50")
print([r[0] for r in cur.fetchall()])

print("\n--- Samples from common area names ---")
cur.execute("SELECT DISTINCT area_name FROM master_sale LIMIT 20")
print([r[0] for r in cur.fetchall()])

cur.close()
conn.close()
