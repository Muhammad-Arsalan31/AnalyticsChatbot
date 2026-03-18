import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(dsn)
cur = conn.cursor()

cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'targets'")
columns = cur.fetchall()
print("TARGETS COLUMNS:")
for col in columns:
    print(f" - {col[0]} ({col[1]})")

cur.execute("SELECT * FROM targets LIMIT 1")
print("\nSAMPLE ROW:")
print(cur.fetchone())

conn.close()
