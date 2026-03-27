import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Master Sale Customer Types ---")
cur.execute("SELECT DISTINCT customer_type FROM master_sale")
print([r[0] for r in cur.fetchall()])

cur.close()
conn.close()
