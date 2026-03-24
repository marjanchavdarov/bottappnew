import os
import zipfile
import pandas as pd
import requests
import json
import time
import math
import gc
import re
from datetime import date
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def get_latest_zip_url():
    print("🔎 Checking api.cijene.dev for the latest data...")
    try:
        r = requests.get("https://api.cijene.dev/v0/list", timeout=20)
        if r.status_code == 200:
            archives = r.json().get("archives", [])
            if archives:
                print(f"✅ Found archive from: {archives[0]['date']}")
                return archives[0]['url']
    except Exception as e:
        print(f"❌ Error fetching ZIP list: {e}")
    return None

def sanitize_num(val):
    if pd.isna(val) or val == "" or val is None: return None
    try:
        s = str(val).replace(',', '.')
        match = re.search(r"[-+]?\d*\.\d+|\d+", s)
        if match:
            f = float(match.group())
            return None if math.isnan(f) or math.isinf(f) else f
    except: pass
    return None

def bulk_upsert(table, data):
    if not data: return True
    # Define unique columns to prevent duplicates
    conflicts = "barcode,store,price_date" if table == "store_prices" else "chain,store_id"
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflicts}"
    
    headers = {
        "apikey": SUPABASE_KEY, 
        "Authorization": f"Bearer {SUPABASE_KEY}", 
        "Content-Type": "application/json", 
        "Prefer": "resolution=merge-duplicates"
    }
    
    # Convert NaNs to None for JSON compatibility
    clean = json.loads(json.dumps(data, default=lambda o: None if isinstance(o, float) and (math.isnan(o) or math.isinf(o)) else o))
    
    try:
        r = requests.post(url, headers=headers, json=clean, timeout=45)
        if r.status_code >= 400:
            print(f"   ❌ Supabase Error: {r.text}")
            return False
        return True
    except Exception as e:
        print(f"   ❌ Connection Error during Upload: {e}")
        return False

def process_zip(zip_path):
    today = date.today().isoformat()
    print(f"📅 Today's Date: {today}")
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Filter folders (store names)
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        print(f"📁 Found {len(folders)} stores to process.")
        
        for store in folders:
            print(f"\n🚀 Processing: {store.upper()}")
            
            # --- 1. STORES (LOCATIONS) ---
            try:
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str)
                s_recs = []
                for _, row in df_s.iterrows():
                    lat = sanitize_num(row.get('lat'))
                    lon = sanitize_num(row.get('lon'))
                    entry = {
                        "store_id": row['store_id'],
                        "chain": store.capitalize(),
                        "address": row.get('address', ''),
                        "city": row.get('city', ''),
                        "zipcode": row.get('zipcode', '')
                    }
                    if lat and lon: entry["location"] = f"POINT({lon} {lat})"
                    s_recs.append(entry)
                
                if bulk_upsert("stores", s_recs):
                    print(f"   ✅ Saved {len(s_recs)} locations.")
            except Exception as e:
                print(f"   ⚠️ Skipping locations for {store}: {e}")

            # --- 2. PRICES (BARCODE MAPPING + CHUNKING) ---
            try:
                # Load the barcode mapping for this store
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), usecols=['product_id', 'barcode'], dtype=str)
                df_p['barcode'] = df_p['barcode'].fillna('').astype(str).str.split('.').str[0].str.strip()
                
                # Use store metadata for the store name string
                df_s_map = pd.read_csv(z.open(f"{store}/stores.csv"), usecols=['store_id', 'city'], dtype=str)

                # Process prices in chunks to save RAM
                p_iter = pd.read_csv(z.open(f"{store}/prices.csv"), chunksize=2000, dtype=str)
                for i, chunk in enumerate(p_iter):
                    merged = chunk.merge(df_p, on='product_id').merge(df_s_map, on='store_id')
                    recs = []
                    for _, row in merged.iterrows():
                        curr_p = sanitize_num(row.get('price'))
                        if curr_p and row.get('barcode'):
                            recs.append({
                                "barcode": row['barcode'],
                                "store": f"{store.capitalize()} - {row['city']}",
                                "price_date": today,
                                "current_price": curr_p,
                                "regular_price": sanitize_num(row.get('anchor_price')) or curr_p
                            })
                    
                    if recs:
                        if bulk_upsert("store_prices", recs):
                            print(f"   🔹 Chunk {i}: Uploaded {len(recs)} prices.")
                    
                    del chunk, merged, recs; gc.collect()
            except Exception as e:
                print(f"   ⚠️ Error processing prices for {store}: {e}")

def main():
    start_time = time.time()
    zip_url = get_latest_zip_url()
    if not zip_url:
        print("🛑 No data archive found for today.")
        return

    print(f"⬇️ Downloading data from {zip_url}...")
    try:
        r = requests.get(zip_url, timeout=120)
        with open("temp.zip", "wb") as f:
            f.write(r.content)
        print("✅ Download finished.")
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return
    
    process_zip("temp.zip")
    
    if os.path.exists("temp.zip"):
        os.remove("temp.zip")
        
    duration = (time.time() - start_time) / 60
    print(f"\n🏁 Finished! Total time: {duration:.2f} minutes.")

if __name__ == "__main__":
    main()
