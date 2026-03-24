import os, zipfile, pandas as pd, requests, json, math, gc
from datetime import date
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def sanitize_num(val):
    if pd.isna(val) or val == "" or val is None: return None
    try:
        s = str(val).replace(',', '.')
        # Keep only digits and decimal point
        f = float("".join(c for c in s if c.isdigit() or c == '.'))
        return None if math.isnan(f) or math.isinf(f) else f
    except: return None

def bulk_upsert(table, data):
    if not data: return
    # barcode is the unique key for products; 
    # barcode+store+date is the unique key for prices.
    conf = "barcode" if table == "master_products" else "barcode,store,price_date"
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conf}"
    
    headers = {
        "apikey": SUPABASE_KEY, 
        "Authorization": f"Bearer {SUPABASE_KEY}", 
        "Content-Type": "application/json", 
        "Prefer": "resolution=merge-duplicates" # CRITICAL: This forces the OVERWRITE of 'New Item'
    }
    
    try:
        r = requests.post(url, headers=headers, json=data, timeout=60)
        if r.status_code >= 400:
            print(f"   ❌ API Error ({table}): {r.text}")
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")

def process_zip(zip_path):
    today = date.today().isoformat()
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Get all store folders
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        for store in folders:
            print(f"🚀 SYNCING: {store.upper()}")
            try:
                # 1. LOAD PRODUCT DATA
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), dtype=str)
                df_p['barcode'] = df_p['barcode'].fillna('').astype(str).str.split('.').str[0].str.strip()
                
                # 2. UPDATE NAMES (Replacing 'New Item' with Real Names)
                valid_prods = df_p[(df_p['name'].notna()) & (df_p['barcode'] != '')].drop_duplicates('barcode')
                p_recs = [{"barcode": r['barcode'], "name": r['name'], "brand": r.get('brand', '')} for _, r in valid_prods.iterrows()]
                
                if p_recs:
                    # We do this in smaller chunks to ensure Supabase handles the 'Merge' correctly
                    for i in range(0, len(p_recs), 2000):
                        bulk_upsert("master_products", p_recs[i:i+2000])

                # 3. LOAD STORES & PRICES
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str)
                df_prices = pd.read_csv(z.open(f"{store}/prices.csv"), dtype=str)
                
                # Merge in Python memory (Fast)
                merged = df_prices.merge(df_p[['product_id', 'barcode']], on='product_id')
                merged = merged.merge(df_s[['store_id', 'city']], on='store_id')

                # 4. PREPARE PRICE UPLOADS
                price_uploads = []
                for _, row in merged.iterrows():
                    p = sanitize_num(row.get('price'))
                    if p and row.get('barcode'):
                        price_uploads.append({
                            "barcode": row['barcode'],
                            "store": f"{store.capitalize()} - {row['city']}",
                            "price_date": today,
                            "current_price": p,
                            "regular_price": sanitize_num(row.get('anchor_price')) or p
                        })
                
                # 5. SEND PRICES IN BATCHES
                if price_uploads:
                    for i in range(0, len(price_uploads), 4000):
                        bulk_upsert("store_prices", price_uploads[i:i+4000])
                
                # Clean RAM
                del df_p, df_s, df_prices, merged, price_uploads; gc.collect()
                print(f"   ✅ {store.upper()} Complete.")

            except Exception as e:
                print(f"   ⚠️ Skipping {store}: {e}")

def main():
    print("🌍 Fetching latest data URL...")
    try:
        r = requests.get("https://api.cijene.dev/v0/list", timeout=20).json()
        url = r['archives'][0]['url']
        print(f"⬇️ Downloading: {url}")
        res = requests.get(url, timeout=180)
        with open("temp.zip", "wb") as f: f.write(res.content)
        
        process_zip("temp.zip")
        
        if os.path.exists("temp.zip"): os.remove("temp.zip")
        print("🏁 ALL STORES SYNCED AND NAMES REPAIRED.")
    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")

if __name__ == "__main__":
    main()
