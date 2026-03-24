import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(dsn)
cur = conn.cursor()

tables_to_check = ['regions', 'zones', 'areas', 'territories', 'teams', 'ims_brick']
term = '%RM - KHI - 1%'

print(f"SEARCHING FOR {term}...")
for table in tables_to_check:
    try:
        cur.execute(f"SELECT * FROM {table} WHERE name ILIKE %s LIMIT 5", (term,))
        res = cur.fetchall()
        if res:
            print(f"FOUND IN {table.upper()}:")
            for r in res:
                print(r)
            print("-----------------------")
    except Exception as e:
        conn.rollback()
        pass

conn.close()
