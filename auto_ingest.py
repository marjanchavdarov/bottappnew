import os, zipfile, pandas as pd, requests, json, time, math, gc, re
from datetime import date
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def sanitize_num(val):
    if pd.isna(val) or val == "" or val is None: return None
    try:
        s = str(val).replace(',', '.')
        s = "".join(c for c in s if c.isdigit() or c == '.')
        f = float(s)
        return None if math.isnan(f) or math.isinf(f) else f
    except: return None

def bulk_upsert(table, data):
    if not data: return True
    # Determine conflict columns based on table
    if table == "store_prices": conf = "barcode,store,price_date"
    elif table == "master_products": conf = "barcode"
    else: conf = "chain,store_id"

    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conf}"
    headers = {
        "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"
    }
    try:
        # Clean data for JSON compliance
        clean = json.loads(json.dumps(data, default=lambda o: None if isinstance(o, float) and (math.isnan(o) or math.isinf(o)) else o))
        r = requests.post(url, headers=headers, json=clean, timeout=45)
        return r.status_code < 400
    except Exception as e:
        print(f"      ❌ API Error: {e}")
        return False

def process_zip(zip_path):
    today = date.today().isoformat()
    with zipfile.ZipFile(zip_path, 'r') as z:
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        for store in folders:
            print(f"\n🚀 Processing: {store.upper()}")
            
            # --- 1. STORES ---
            try:
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str)
                s_recs = [{"store_id": r['store_id'], "chain": store.capitalize(), "address": r.get('address',''), "city": r.get('city','')} for _, r in df_s.iterrows()]
                bulk_upsert("stores", s_recs)
            except: pass

            # --- 2. PRICES & AUTO-REGISTER PRODUCTS ---
            try:
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), usecols=['product_id', 'barcode'], dtype=str)
                df_p['barcode'] = df_p['barcode'].fillna('').astype(str).str.split('.').str[0].str.strip()
                
                p_iter = pd.read_csv(z.open(f"{store}/prices.csv"), chunksize=2000, dtype=str)
                for i, chunk in enumerate(p_iter):
                    merged = chunk.merge(df_p, on='product_id').merge(df_s[['store_id', 'city']], on='store_id')
                    
                    # A. Register new barcodes so Foreign Key doesn't break
                    new_prods = [{"barcode": b, "name": "New Item"} for b in merged['barcode'].unique() if b]
                    bulk_upsert("master_products", new_prods)

                    # B. Upload Prices
                    recs = []
                    for _, row in merged.iterrows():
                        p = sanitize_num(row.get('price'))
                        if p and row.get('barcode'):
                            recs.append({
                                "barcode": row['barcode'],
                                "store": f"{store.capitalize()} - {row['city']}",
                                "price_date": today,
                                "current_price": p,
                                "regular_price": sanitize_num(row.get('anchor_price')) or p
                            })
                    if recs:
                        bulk_upsert("store_prices", recs)
                        print(f"   ✅ Chunk {i} finished.")
                    
                    del chunk, merged; gc.collect()
            except Exception as e:
                print(f"   ⚠️ Store Error: {e}")

def main():
    print("🔎 Finding latest ZIP...")
    r = requests.get("https://api.cijene.dev/v0/list", timeout=20)
    url = r.json()['archives'][0]['url']
    
    print(f"⬇️ Downloading...")
    r = requests.get(url, timeout=120)
    with open("temp.zip", "wb") as f: f.write(r.content)
    
    process_zip("temp.zip")
    if os.path.exists("temp.zip"): os.remove("temp.zip")
    print("🏁 Done!")

if __name__ == "__main__":
    main()
