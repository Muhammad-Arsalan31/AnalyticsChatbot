import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL').split('?')[0])
cur = conn.cursor()

cur.execute("SELECT name FROM zones WHERE region_id=1")
print("ZONES:", [r[0] for r in cur.fetchall()])

cur.execute("SELECT name FROM areas WHERE region_id=1 LIMIT 5")
print("AREAS:", [r[0] for r in cur.fetchall()])

conn.close()
