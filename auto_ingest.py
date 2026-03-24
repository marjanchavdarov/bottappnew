import os
import zipfile
import pandas as pd
import requests
import gc
import math
from datetime import date
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def safe_float(val):
    """Specific rule for prices: handles commas and ignores non-numbers."""
    try:
        if pd.isna(val) or val == "": 
            return None
        s = str(val).replace(',', '.')
        f = float("".join(c for c in s if c.isdigit() or c == '.'))
        return f if math.isfinite(f) else None
    except: 
        return None

def bulk_upsert(table, data):
    if not data: 
        return
    
    # Determine conflict columns based on table
    if table == "master_products":
        conf = "barcode"
    else:
        conf = "barcode,store,price_date"
    
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conf}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    
    try:
        r = requests.post(url, headers=headers, json=data, timeout=60)
        if r.status_code >= 400:
            print(f"   ❌ {table} Error: {r.text[:200]}")
        else:
            print(f"   ✅ {table} upserted {len(data)} records")
    except Exception as e:
        print(f"   ❌ {table} Request failed: {e}")

def process_zip(zip_path):
    today = date.today().isoformat()
    
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Get all top-level folders (stores)
        folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
        
        print(f"📁 Found {len(folders)} stores to process")
        
        for store in folders:
            print(f"\n🏪 Shop: {store.upper()}")
            
            try:
                # 1. Read CSV files
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), dtype=str).fillna('')
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), dtype=str).fillna('')
                df_prices = pd.read_csv(z.open(f"{store}/prices.csv"), dtype=str).fillna('')
                
                print(f"   📊 Products: {len(df_p)}, Stores: {len(df_s)}, Prices: {len(df_prices)}")
                
                # 2. Clean barcodes - just strip whitespace, don't split
                df_p['barcode'] = df_p['barcode'].astype(str).str.strip()
                df_p = df_p[df_p['barcode'] != '']
                
                # 3. Deduplicate products by barcode for master_products
                catalog = df_p.drop_duplicates('barcode')[['barcode', 'name']].copy()
                
                # 4. Upsert master_products
                if not catalog.empty:
                    p_recs = catalog.to_dict(orient='records')
                    print(f"   📦 Upserting {len(p_recs)} products to master_products")
                    for i in range(0, len(p_recs), 1000):
                        bulk_upsert("master_products", p_recs[i:i+1000])
                
                # 5. Prepare for price merging - deduplicate products and stores
                unique_products = df_p.drop_duplicates('product_id')[['product_id', 'barcode']]
                unique_stores = df_s.drop_duplicates('store_id')[['store_id', 'city']]
                
                # 6. Merge all data
                merged = df_prices.merge(unique_products, on='product_id', how='inner')
                merged = merged.merge(unique_stores, on='store_id', how='inner')
                
                print(f"   🔗 Merged {len(merged)} price records")
                
                # 7. Build final price records
                final_prices = []
                for _, row in merged.iterrows():
                    p_val = safe_float(row.get('price'))
                    if p_val is not None and row['barcode']:
                        # Format store name
                        store_name = f"{store.capitalize()} - {row['city']}"
                        final_prices.append({
                            "barcode": row['barcode'],
                            "store": store_name,
                            "price_date": today,
                            "current_price": p_val
                        })
                
                # 8. Deduplicate prices
                if final_prices:
                    price_df = pd.DataFrame(final_prices)
                    clean_prices = price_df.drop_duplicates(
                        subset=['barcode', 'store', 'price_date']
                    ).to_dict(orient='records')
                    
                    print(f"   💰 Upserting {len(clean_prices)} price records")
                    
                    # Upsert in batches
                    for i in range(0, len(clean_prices), 3000):
                        bulk_upsert("store_prices", clean_prices[i:i+3000])
                else:
                    print(f"   ⚠️ No valid prices found")
                
                print(f"   ✅ Store {store.upper()} completed")
                
                # Clean up
                del df_p, df_s, df_prices, merged, catalog
                gc.collect()
                
            except KeyError as e:
                print(f"   ⚠️ Missing file in {store}: {e}")
            except Exception as e:
                print(f"   ⚠️ Error processing {store}: {e}")
                import traceback
                traceback.print_exc()

def main():
    print("🌍 Starting data sync...")
    print("=" * 50)
    
    # Check environment variables
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment variables")
        return
    
    print(f"✅ Supabase URL: {SUPABASE_URL}")
    
    try:
        # Download latest data
        print("📡 Fetching latest data from API...")
        r = requests.get("https://api.cijene.dev/v0/list", timeout=20)
        r.raise_for_status()
        data = r.json()
        
        if not data.get('archives'):
            print("❌ No archives found in API response")
            return
        
        # Get the latest archive
        archive = data['archives'][0]
        url = archive.get('url')
        
        if not url:
            print("❌ No URL found in archive")
            return
        
        print(f"📥 Downloading from: {url}")
        res = requests.get(url, timeout=300)
        res.raise_for_status()
        
        # Save zip file
        zip_path = "data.zip"
        with open(zip_path, "wb") as f:
            f.write(res.content)
        
        print(f"✅ Downloaded {len(res.content) / 1024 / 1024:.2f} MB")
        
        # Process the zip
        print("\n📦 Processing zip file...")
        process_zip(zip_path)
        
        # Clean up
        if os.path.exists(zip_path):
            os.remove(zip_path)
            print("\n🗑️ Cleaned up temporary files")
        
        print("\n" + "=" * 50)
        print("🏁 SYNC COMPLETE!")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
