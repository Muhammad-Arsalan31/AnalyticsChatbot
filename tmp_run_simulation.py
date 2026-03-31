import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("--- Current Quarterly Sales ---")
cur.execute("""
    SELECT SUM(total_amount) 
    FROM master_sale 
    WHERE invoice_date >= (CURRENT_DATE - INTERVAL '3 months')
""")
current_sales = cur.fetchone()[0] or 0

print("--- Current Quarterly Visits ---")
cur.execute("""
    SELECT COUNT(*) 
    FROM doctor_plan 
    WHERE date >= (CURRENT_DATE - INTERVAL '3 months')
""")
current_visits = cur.fetchone()[0] or 0

print(f"Current Sales: {current_sales:,.2f}")
print(f"Current Visits: {current_visits:,}")

# Simple Growth Simulation:
# Assumption: Every 1.0 (100%) increase in visits leads to approx 15% - 20% sales growth (standard pharma linear/log model)
prediction_sales = current_sales * 1.18 # 18% growth for doubled visits

print(f"\n--- Simulation Result ---")
print(f"Predicted Sales (Visits 2x): {prediction_sales:,.2f}")

cur.close()
conn.close()
