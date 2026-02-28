import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL").split('?')[0]

def diagnose():
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        
        # Get all tables
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        db_tables = [t[0] for t in cur.fetchall()]
        
        # Get all columns for each table
        db_schema = {}
        for table in db_tables:
            cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}';")
            db_schema[table] = {c[0]: c[1] for c in cur.fetchall()}
            
        conn.close()
        return db_schema
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    schema_info = diagnose()
    with open("db_reality.json", "w") as f:
        import json
        json.dump(schema_info, f, indent=4)
    print("Database Reality Scanned and saved to db_reality.json")
