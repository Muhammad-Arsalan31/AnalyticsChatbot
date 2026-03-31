import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL').split('?')[0])
cur = conn.cursor()
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='managers';")
print('Managers columns:', cur.fetchall())
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='users';")
print('Users columns:', cur.fetchall())
conn.close()
