import os, zipfile, pandas as pd, requests, json, time, math, gc, re
from datetime import date
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def get_latest_zip_url():
    """Fetches the URL for today's data archive from the API."""
    try:
        r = requests.get("https://api.cijene.dev/v0/list")
        if r.status_code == 200:
            archives = r.json().get("archives", [])
            if archives:
                # The first item is the most recent
                print(f"🔎 Found latest archive from {archives[0]['date']}")
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
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={'barcode,store,price_date' if table == 'store_prices' else 'barcode'}"
    headers = {
        "apikey": SUPABASE_KEY, 
        "Authorization": f"Bearer {SUPABASE_KEY}", 
        "Content-Type": "application/json", 
        "Prefer": "resolution=merge-duplicates"
    }
    # Clean for JSON (NaN to None)
    clean = json.loads(json.dumps(data, default=lambda o: None if isinstance(o, float) and (math.isnan(o) or math.isinf(o)) else o))
    try:
        r = requests.post(url, headers=headers, json=clean, timeout=60)
        return r.status_code < 400
    except: return False

def process_zip(zip_path):
    today = date.today().isoformat()
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Get list of store folders
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        for store in folders:
            print(f"📦 Processing Store: {store.upper()}")
            
            # 1. PROCESS STORES (LOCATIONS)
            try:
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str)
                s_recs = []
                for _, row in df_s.iterrows():
                    s_recs.append({
                        "store_id": row['store_id'],
                        "chain": store.capitalize(),
                        "address": row.get('address', ''),
                        "city": row.get('city', ''),
                        "zipcode": row.get('zipcode', '')
                    })
                bulk_upsert("stores", s_recs) # Make sure you create this table!
            except: pass

            # 2. PROCESS PRICES (CHUNKED FOR MEMORY)
            try:
                # Load product mapping (barcode <-> id)
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), usecols=['product_id', 'barcode'], dtype=str)
                df_p['barcode'] = df_p['barcode'].fillna('').astype(str).str.split('.').str[0].str.strip()
                
                # Read prices in chunks of 1000
                p_iter = pd.read_csv(z.open(f"{store}/prices.csv"), chunksize=1000, dtype=str)
                for chunk in p_iter:
                    merged = chunk.merge(df_p, on='product_id').merge(df_s, on='store_id')
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
                    bulk_upsert("store_prices", recs)
                    del chunk, merged, recs; gc.collect()
            except Exception as e:
                print(f"⚠️ Error in {store}: {e}")

def main():
    zip_url = get_latest_zip_url()
    if not zip_url:
        print("🛑 No ZIP found today.")
        return

    print(f"⬇️ Downloading ZIP...")
    r = requests.get(zip_url)
    with open("temp.zip", "wb") as f:
        f.write(r.content)
    
    process_zip("temp.zip")
    os.remove("temp.zip")
    print("🏁 Automation Complete.")

if __name__ == "__main__":
    main()
