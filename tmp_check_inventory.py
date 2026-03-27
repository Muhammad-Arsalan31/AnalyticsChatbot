import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- ALL unique product_group_name in master_sale ---")
cur.execute("SELECT DISTINCT product_group_name FROM master_sale")
print([r[0] for r in cur.fetchall()])

print("\n--- ALL unique area_name in master_sale ---")
cur.execute("SELECT DISTINCT area_name FROM master_sale")
print([r[0] for r in cur.fetchall()])

cur.close()
conn.close()
