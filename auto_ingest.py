import os, zipfile, pandas as pd, requests, gc, math
from datetime import date
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def safe_float(val):
    """Specific rule for prices: handles commas and ignores non-numbers."""
    try:
        if pd.isna(val) or val == "": return None
        s = str(val).replace(',', '.')
        f = float("".join(c for c in s if c.isdigit() or c == '.'))
        return f if math.isfinite(f) else None
    except: return None

def bulk_upsert(table, data):
    if not data: return
    conf = "barcode" if table == "master_products" else "barcode,store,price_date"
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conf}"
    headers = {
        "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"
    }
    r = requests.post(url, headers=headers, json=data, timeout=60)
    if r.status_code >= 400:
        print(f"   ❌ {table} Error: {r.text[:100]}")

def process_zip(zip_path):
    today = date.today().isoformat()
    with zipfile.ZipFile(zip_path, 'r') as z:
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        for store in folders:
            print(f"🏪 Shop: {store.upper()}")
            try:
                # 1. Individual Load: If one file is missing, only this shop skips
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), dtype=str).fillna('')
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str).fillna('')
                df_prices = pd.read_csv(z.open(f"{store}/prices.csv"), dtype=str).fillna('')

                # 2. Rule: Clean barcodes specifically for this shop
                df_p['barcode'] = df_p['barcode'].str.split('.').str[0].str.strip()
                df_p = df_p[df_p['barcode'] != '']

                # 3. Rule: Repair 'New Item' names
                catalog = df_p.drop_duplicates('barcode')[['barcode', 'name']]
                p_recs = catalog.to_dict(orient='records')
                for i in range(0, len(p_recs), 1000):
                    bulk_upsert("master_products", p_recs[i:i+1000])

                # 4. Rule: Merge and build unique price rows
                merged = df_prices.merge(df_p[['product_id', 'barcode']], on='product_id')
                merged = merged.merge(df_s[['store_id', 'city']], on='store_id')

                final_prices = []
                for _, row in merged.iterrows():
                    p_val = safe_float(row.get('price'))
                    if p_val is not None and row['barcode']:
                        final_prices.append({
                            "barcode": row['barcode'],
                            "store": f"{store.capitalize()} - {row['city']}",
                            "price_date": today,
                            "current_price": p_val
                        })

                # 5. Rule: Deduplicate BEFORE sending (Kills the 21000 error)
                if final_prices:
                    clean_data = pd.DataFrame(final_prices).drop_duplicates(
                        subset=['barcode', 'store', 'price_date']
                    ).to_dict(orient='records')
                    
                    for i in range(0, len(clean_data), 3000):
                        bulk_upsert("store_prices", clean_data[i:i+3000])
                
                print(f"   ✅ Done.")
                del df_p, df_s, df_prices, merged; gc.collect()

            except Exception as e:
                print(f"   ⚠️ Skipping {store}: {e}")

def main():
    print("🌍 Fetching Latest Data...")
    try:
        r = requests.get("https://api.cijene.dev/v0/list", timeout=20).json()
        url = r['archives'][0]['url']
        res = requests.get(url, timeout=300)
        with open("data.zip", "wb") as f: f.write(res.content)
        process_zip("data.zip")
        if os.path.exists("data.zip"): os.remove("data.zip")
        print("🏁 SYNC COMPLETE.")
    except Exception as e:
        print(f"❌ FATAL: {e}")

if __name__ == "__main__":
    main()
