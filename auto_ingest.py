import os, zipfile, pandas as pd, requests, json, time, math, gc
from datetime import date
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def bulk_upsert(table, data):
    if not data: return
    # Identify the unique key for each table to avoid duplicates
    conf = "barcode" if table == "master_products" else ("barcode,store,price_date" if table == "store_prices" else "chain,store_id")
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conf}"
    headers = {
        "apikey": SUPABASE_KEY, 
        "Authorization": f"Bearer {SUPABASE_KEY}", 
        "Content-Type": "application/json", 
        "Prefer": "resolution=merge-duplicates"
    }
    try:
        # We use a 60s timeout to allow large batches to process
        requests.post(url, headers=headers, json=data, timeout=60)
    except: pass

def process_zip(zip_path):
    today = date.today().isoformat()
    with zipfile.ZipFile(zip_path, 'r') as z:
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        for store in folders:
            print(f"⚡ FAST SYNC: {store.upper()}")
            try:
                # 1. LOAD DATA & CLEAN BARCODES IMMEDIATELY
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), usecols=['product_id', 'barcode', 'name', 'brand'], dtype=str)
                df_p['barcode'] = df_p['barcode'].fillna('').astype(str).str.split('.').str[0].str.strip()
                
                # 2. UPDATE PRODUCT NAMES (The fix for 'New Item')
                # We only take rows that actually have a barcode and a name
                valid_prods = df_p[(df_p['name'].notna()) & (df_p['barcode'] != '')].drop_duplicates('barcode')
                p_recs = [{"barcode": r['barcode'], "name": r['name'], "brand": r.get('brand', '')} for _, r in valid_prods.iterrows()]
                
                # Send all products for this store in one single shot
                if p_recs: bulk_upsert("master_products", p_recs)

                # 3. UPLOAD PRICES IN MASSIVE CHUNKS (10,000 at a time)
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str)
                p_iter = pd.read_csv(z.open(f"{store}/prices.csv"), chunksize=10000, dtype=str)
                
                for chunk in p_iter:
                    merged = chunk.merge(df_p, on='product_id').merge(df_s[['store_id', 'city']], on='store_id')
                    recs = []
                    for _, row in merged.iterrows():
                        p = float(str(row['price']).replace(',','.')) if pd.notna(row.get('price')) else None
                        if p and row.get('barcode'):
                            recs.append({
                                "barcode": row['barcode'],
                                "store": f"{store.capitalize()} - {row['city']}",
                                "price_date": today,
                                "current_price": p,
                                "regular_price": p
                            })
                    if recs: bulk_upsert("store_prices", recs)
                    del chunk, merged; gc.collect()
            except: continue

if __name__ == "__main__":
    # Get the URL and run
    r = requests.get("https://api.cijene.dev/v0/list").json()
    url = r['archives'][0]['url']
    res = requests.get(url)
    with open("temp.zip", "wb") as f: f.write(res.content)
    process_zip("temp.zip")
    print("🏁 FINISHED.")
