import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
DB_URL = os.getenv("DATABASE_URL").split('?')[0]

def check_gulshan():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    print("--- BRICKS with 'GULSHAN' in Name ---")
    cur.execute("SELECT id, name FROM ims_brick WHERE name ILIKE '%GULSHAN%'")
    bricks = cur.fetchall()
    for b in bricks:
        print(f"ID: {b[0]}, Name: {b[1]}")
    
    if not bricks:
        print("NO BRICKS FOUND WITH 'GULSHAN'")
        return

    brick_ids = [b[0] for b in bricks]
    
    print("\n--- Sales (Invoices) for these Bricks ---")
    # This matches the Agent's join logic
    cur.execute("""
        SELECT ib.name, COUNT(inv.id)
        FROM invoice inv
        JOIN customer_details cd ON inv.cust_id = cd.customer_id
        JOIN ims_brick ib ON cd.ims_brick_id = ib.id
        WHERE ib.id IN %s
        GROUP BY ib.name
    """, (tuple(brick_ids),))
    sales = cur.fetchall()
    if not sales:
        print("NO INVOICE DATA FOR GULSHAN BRICKS!")
    else:
        for s in sales:
            print(f"Brick: {s[0]}, Invoices: {s[1]}")
            
    print("\n--- TOP BRICKS WITH SALES (Total) ---")
    cur.execute("""
        SELECT ib.name, COUNT(inv.id) as c
        FROM invoice inv
        JOIN customer_details cd ON inv.cust_id = cd.customer_id
        JOIN ims_brick ib ON cd.ims_brick_id = ib.id
        GROUP BY ib.name
        ORDER BY c DESC
        LIMIT 10
    """)
    top_bricks = cur.fetchall()
    for tb in top_bricks:
        print(f"Top Brick: {tb[0]}, Invoices: {tb[1]}")

    conn.close()

if __name__ == "__main__":
    check_gulshan()
