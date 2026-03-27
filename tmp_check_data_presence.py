import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Checking Antibiotics in Master Sale ---")
cur.execute("SELECT DISTINCT product_group_name FROM master_sale WHERE product_group_name ILIKE '%Antibiotic%'")
print([r[0] for r in cur.fetchall()])

print("\n--- Checking Karachi in Master Sale ---")
cur.execute("SELECT DISTINCT area_name FROM master_sale WHERE area_name ILIKE '%Karachi%'")
print([r[0] for r in cur.fetchall()])

print("\n--- Checking Intersection (Antibiotics + Karachi) ---")
cur.execute("SELECT product_group_name, area_name, COUNT(*) FROM master_sale WHERE product_group_name ILIKE '%Antibiotic%' AND area_name ILIKE '%Karachi%' GROUP BY 1, 2")
print(cur.fetchall())

cur.close()
conn.close()
