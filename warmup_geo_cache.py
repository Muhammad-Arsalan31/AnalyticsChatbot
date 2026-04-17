"""
Geo-Cache Warmup Script
========================
Ye script database ke saaray CUSTOMERS aur HEALTHCENTRES 
ka address fetch kar ke geo_cache.json mein save karti hai.
Is se Dashboard Map foran load hota hai.
"""

import os
import json
import time
import urllib.request
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "").split("?")[0]
CHATS_DIR = os.path.join(os.getcwd(), "chats", "admin@gmail.com") # Default path
CACHE_FILE = os.path.join(CHATS_DIR, "geo_cache.json")

os.makedirs(CHATS_DIR, exist_ok=True)

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f: return json.load(f)
        except: return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=4)

def reverse_geocode(lat, lng):
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat:.4f}&lon={lng:.4f}&zoom=18"
        req = urllib.request.Request(url, headers={"User-Agent": "PharmaWarmup/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            return data.get("display_name", "Unknown")
    except Exception as e:
        print(f"  Err: {e}")
        return None

def main():
    print("🚀 Starting Geo-Cache Warmup...")
    cache = load_cache()
    print(f"📖 Existing cache: {len(cache)} addresses")

    conn = psycopg2.connect(DB_URL)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get all coordinates
        cur.execute("""
            SELECT latitude::float, longitude::float, name FROM customers WHERE latitude IS NOT NULL
            UNION
            SELECT latitude::float, longitude::float, name FROM healthcentres WHERE latitude IS NOT NULL
        """)
        rows = cur.fetchall()

    print(f"🎯 Total records to check: {len(rows)}")
    
    new_count = 0
    try:
        for i, row in enumerate(rows):
            lat, lng = row['latitude'], row['longitude']
            key = f"{lat:.4f},{lng:.4f}"
            
            if key not in cache:
                print(f"[{i+1}/{len(rows)}] Geocoding: {row['name']}...")
                addr = reverse_geocode(lat, lng)
                if addr:
                    cache[key] = addr
                    new_count += 1
                    # Save every 5 records to be safe
                    if new_count % 5 == 0: save_cache(cache)
                    
                    # Respectful timeout (1 sec per request is Nominatim's policy)
                    time.sleep(1.1) 
            else:
                if i % 50 == 0: print(f"[{i+1}/{len(rows)}] Already in cache: {row['name']}")

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user. Saving progress...")

    save_cache(cache)
    print(f"\n✅ Warmup Complete! Added {new_count} new addresses.")
    print(f"💾 Total Cache Size: {len(cache)} entries.")
    conn.close()

if __name__ == "__main__":
    main()
