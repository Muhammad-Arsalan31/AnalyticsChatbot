import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL").split('?')[0]
conn = psycopg2.connect(dsn)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'master_sale'")
cols = [c[0] for c in cur.fetchall()]
print(cols)
conn.close()
