import os, zipfile, pandas as pd, requests, json, gc
from datetime import date
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def bulk_upsert(table, data):
    if not data: return
    # Use the unique keys to prevent duplicates
    conf = "barcode" if table == "master_products" else "barcode,store,price_date"
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conf}"
    headers = {
        "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"
    }
    # Send up to 5,000 rows in a single request
    requests.post(url, headers=headers, json=data, timeout=60)

def process_zip(zip_path):
    today = date.today().isoformat()
    with zipfile.ZipFile(zip_path, 'r') as z:
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        for store in folders:
            print(f"⚡ FAST SYNCing: {store.upper()}")
            try:
                # 1. LOAD PRODUCTS & STORES INTO MEMORY
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), dtype=str)
                df_p['barcode'] = df_p['barcode'].fillna('').astype(str).str.split('.').str[0].str.strip()
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str)
                
                # 2. FIX NAMES (The 'New Item' killer)
                valid_prods = df_p[(df_p['name'].notna()) & (df_p['barcode'] != '')].drop_duplicates('barcode')
                p_recs = [{"barcode": r['barcode'], "name": r['name'], "brand": r.get('brand', '')} for _, r in valid_prods.iterrows()]
                if p_recs: bulk_upsert("master_products", p_recs)

                # 3. FAST MERGE IN PYTHON (This saves 40 minutes)
                df_prices = pd.read_csv(z.open(f"{store}/prices.csv"), dtype=str)
                merged = df_prices.merge(df_p[['product_id', 'barcode']], on='product_id')
                merged = merged.merge(df_s[['store_id', 'city']], on='store_id')

                # 4. PREPARE & UPLOAD PRICES
                price_uploads = []
                for _, row in merged.iterrows():
                    try:
                        p = float(str(row['price']).replace(',','.'))
                        if p and row.get('barcode'):
                            price_uploads.append({
                                "barcode": row['barcode'],
                                "store": f"{store.capitalize()} - {row['city']}",
                                "price_date": today,
                                "current_price": p,
                                "regular_price": p
                            })
                    except: continue
                
                # Send in blocks of 5000
                for i in range(0, len(price_uploads), 5000):
                    bulk_upsert("store_prices", price_uploads[i:i+5000])
                
                del df_p, df_s, df_prices, merged, price_uploads; gc.collect()
            except: continue

if __name__ == "__main__":
    r = requests.get("https://api.cijene.dev/v0/list").json()
    url = r['archives'][0]['url']
    res = requests.get(url)
    with open("temp.zip", "wb") as f: f.write(res.content)
    process_zip("temp.zip")
    print("🏁 FINISHED SUCCESSFULLY.")
