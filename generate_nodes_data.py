import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load config
env_path = r"c:\Users\Expo\Desktop\New folder\.env"
load_dotenv(env_path)
DB_URL = os.getenv("DATABASE_URL")

def generate_data():
    if not DB_URL:
        print("Error: DATABASE_URL not found.")
        return

    try:
        # Clean URL for psycopg2
        clean_db_url = DB_URL.split('?')[0]
        conn = psycopg2.connect(clean_db_url)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            nodes = []
            edges = []
            node_ids = set()

            # 1. Get Managers (Level 0)
            cur.execute('SELECT id, name FROM "managers" LIMIT 50')
            for m in cur.fetchall():
                mid = f"m_{m['id']}"
                if mid not in node_ids:
                    nodes.append({
                        "id": mid, 
                        "label": m['name'], 
                        "group": "manager", 
                        "title": f"Manager: {m['name']}",
                        "color": "#6366f1",
                        "size": 25
                    })
                    node_ids.add(mid)

            # 2. Get Doctors and their connections from doctor_plan
            cur.execute('''
                SELECT m.id as mid, d.id as did, d.name as dname, hc.id as hcid, hc.name as hcname
                FROM doctor_plan dp
                JOIN managers m ON dp."managerId" = m.id
                JOIN doctors d ON dp."doctorId" = d.id
                LEFT JOIN healthcentres hc ON dp."healthCentreId" = hc.id
                LIMIT 500
            ''')
            
            rows = cur.fetchall()
            print(f"Fetched {len(rows)} relationship records.")

            for row in rows:
                mid = f"m_{row['mid']}"
                did = f"d_{row['did']}"
                hcid = f"hc_{row['hcid']}" if row['hcid'] else None

                # Add Doctor Node
                if did not in node_ids:
                    nodes.append({
                        "id": did, 
                        "label": row['dname'], 
                        "group": "doctor", 
                        "title": f"Doctor: {row['dname']}",
                        "color": "#a855f7",
                        "size": 18
                    })
                    node_ids.add(did)
                
                # Edge: Manager -> Doctor
                edge_id_md = f"{mid}_{did}"
                # Using a set check for edges would be better, but we'll just add
                edges.append({"from": mid, "to": did, "label": "visits", "color": {"inherit": "from"}})

                # Add Health Centre Node
                if hcid and hcid not in node_ids:
                    nodes.append({
                        "id": hcid, 
                        "label": row['hcname'], 
                        "group": "healthcentre", 
                        "title": f"Clinic: {row['hcname']}",
                        "color": "#10b981",
                        "size": 20
                    })
                    node_ids.add(hcid)
                
                # Edge: Doctor -> Health Centre
                if hcid:
                    edges.append({"from": did, "to": hcid, "label": "practices at", "color": {"inherit": "to"}})

            # Save to JSON
            output = {"nodes": nodes, "edges": edges}
            with open("nodes_data.json", "w") as f:
                json.dump(output, f, indent=4)
            
            print(f"✅ Generated {len(nodes)} nodes and {len(edges)} edges.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_data()
