"""
Coordinates Diagnostic Tool
==============================
Ye script database mein saari tables ke
latitude/longitude data check karti hai.
Run: python check_coordinates.py
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "").split("?")[0]

def connect():
    return psycopg2.connect(DB_URL)

def section(title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print('='*55)

def run(conn, sql, params=None):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

def main():
    try:
        conn = connect()
        print("\n✅ Database Connected Successfully!")
    except Exception as e:
        print(f"\n❌ Connection Failed: {e}")
        return

    # ── 1. CUSTOMERS ──────────────────────────────────────
    section("1. CUSTOMERS TABLE — Coordinate Summary")
    rows = run(conn, """
        SELECT
            COUNT(*)                                        AS total_customers,
            COUNT(latitude)                                 AS has_latitude,
            COUNT(longitude)                                AS has_longitude,
            COUNT(CASE WHEN latitude IS NOT NULL
                        AND longitude IS NOT NULL THEN 1 END) AS both_coords
        FROM customers
    """)
    r = rows[0]
    print(f"  Total Customers   : {r['total_customers']}")
    print(f"  Has Latitude      : {r['has_latitude']}")
    print(f"  Has Longitude     : {r['has_longitude']}")
    print(f"  Both Coords ✅    : {r['both_coords']}")

    section("1b. CUSTOMERS — Sample with Coordinates")
    rows = run(conn, """
        SELECT name, latitude, longitude
        FROM customers
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 5
    """)
    if rows:
        for r in rows:
            print(f"  {r['name']:<35} lat={r['latitude']}, lng={r['longitude']}")
    else:
        print("  ⚠️  No customers have coordinates!")

    # ── 2. SEARCH by name ─────────────────────────────────
    section("2. SEARCH — RAMEEZ CLINIC coordinates")
    rows = run(conn, """
        SELECT name, latitude, longitude
        FROM customers
        WHERE name ILIKE %s
        LIMIT 5
    """, ("%rameez%",))
    if rows:
        for r in rows:
            print(f"  ✅ {r['name']:<35} lat={r['latitude']}, lng={r['longitude']}")
    else:
        print("  ❌ 'RAMEEZ CLINIC' NOT found in customers table")

    # ── 3. HEALTHCENTRES ──────────────────────────────────
    section("3. HEALTHCENTRES TABLE — Coordinate Summary")
    rows = run(conn, """
        SELECT
            COUNT(*)                                        AS total,
            COUNT(latitude)                                 AS has_latitude,
            COUNT(longitude)                                AS has_longitude,
            COUNT(CASE WHEN latitude IS NOT NULL
                        AND longitude IS NOT NULL THEN 1 END) AS both_coords
        FROM healthcentres
    """)
    r = rows[0]
    print(f"  Total HealthCentres : {r['total']}")
    print(f"  Has Latitude        : {r['has_latitude']}")
    print(f"  Has Longitude       : {r['has_longitude']}")
    print(f"  Both Coords ✅      : {r['both_coords']}")

    section("3b. HEALTHCENTRES — Search RAMEEZ")
    rows = run(conn, """
        SELECT name, latitude, longitude
        FROM healthcentres
        WHERE name ILIKE %s
        LIMIT 5
    """, ("%rameez%",))
    if rows:
        for r in rows:
            print(f"  ✅ {r['name']:<35} lat={r['latitude']}, lng={r['longitude']}")
    else:
        print("  ❌ 'RAMEEZ' NOT found in healthcentres table")

    # ── 4. DOCTORS via doctor_plan ────────────────────────
    section("4. DOCTORS — via doctor_plan → healthcentres")
    rows = run(conn, """
        SELECT d.name AS doctor, hc.name AS clinic,
               hc.latitude, hc.longitude
        FROM doctors d
        JOIN doctor_plan dp ON dp."doctorId" = d.id
        JOIN healthcentres hc ON hc.id = dp."healthCentreId"
        WHERE hc.latitude IS NOT NULL
        LIMIT 5
    """)
    if rows:
        for r in rows:
            print(f"  Dr. {r['doctor']:<25} @ {r['clinic']:<25} lat={r['latitude']}, lng={r['longitude']}")
    else:
        print("  ⚠️  No doctor-clinic coordinate links found")

    # ── 5. COLUMN CHECK ───────────────────────────────────
    section("5. COLUMN NAMES — customers & healthcentres")
    for tbl in ["customers", "healthcentres"]:
        rows = run(conn, """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
              AND column_name ILIKE ANY(ARRAY['%lat%','%lng%','%lon%','%coord%'])
        """, (tbl,))
        if rows:
            print(f"\n  [{tbl}]")
            for r in rows:
                print(f"    {r['column_name']:<20} ({r['data_type']})")
        else:
            print(f"\n  [{tbl}] — No lat/lng columns found! ❌")

    conn.close()
    print("\n" + "="*55)
    print("  Diagnostic Complete ✅")
    print("="*55 + "\n")

if __name__ == "__main__":
    main()
