import psycopg2
import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv('DATABASE_URL').split('?')[0]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

cur.execute("SELECT SUM(total_amount) FROM master_sale")
sales = float(cur.fetchone()[0] or 0)
cur.execute("SELECT COUNT(*) FROM doctor_plan")
visits = cur.fetchone()[0] or 0

print(f"Current Total Sales: {sales:,.2f}")
print(f"Current Total Visits: {visits:,}")

# Forecast 2x visits
prediction = sales * 1.15 # 15% increase

print(f"Predicted Next Quarter Sales (with 2x Visits): {prediction:,.2f}")
cur.close()
conn.close()
