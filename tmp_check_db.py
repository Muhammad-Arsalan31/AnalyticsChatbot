import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Customer Types ---")
cur.execute("SELECT type FROM customer_types")
print([r[0] for r in cur.fetchall()])

print("\n--- Any Doctor in Customer Table? ---")
cur.execute("SELECT name FROM customers WHERE name ILIKE '%dr%' LIMIT 5")
print([r[0] for r in cur.fetchall()])

print("\n--- Any Link between doctors and customers? ---")
# Check if customer_id in doctors?
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='doctors'")
print("Doctors columns:", [r[0] for r in cur.fetchall()])

cur.close()
conn.close()
