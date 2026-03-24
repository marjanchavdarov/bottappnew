import os
import time
import zipfile
import threading
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import date
from flask import Flask, jsonify

load_dotenv()

# --- INITIALIZE FLASK (Required for Render to stay alive) ---
app = Flask(__name__)

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
RENDER_URL   = os.environ.get("RENDER_EXTERNAL_URL") # Render provides this automatically

# 1. THE KEEP-ALIVE SYSTEM
def keep_awake():
    """Pings the app every 10 minutes so Render doesn't sleep."""
    if not RENDER_URL:
        print("💡 Not running on Render (no RENDER_EXTERNAL_URL), skipping keep-awake.")
        return
    
    print(f"⏰ Starting Keep-Alive thread for: {RENDER_URL}")
    while True:
        try:
            requests.get(RENDER_URL, timeout=10)
        except Exception:
            pass
        time.sleep(600) # 10 minutes

# Start the thread immediately when the app starts
threading.Thread(target=keep_awake, daemon=True).start()

# 2. THE DATABASE HELPERS
def db_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def bulk_upsert(table, data, batch_size=1000):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        requests.post(url, headers=db_headers(), json=batch)

# 3. THE MASTER ZIP PROCESSOR
@app.route("/run-ingest")
def trigger_ingest():
    """Manual trigger via browser: your-app.onrender.com/run-ingest"""
    zip_path = 'daily_prices_master.zip' # Make sure you upload this to your root folder
    if not os.path.exists(zip_path):
        return jsonify({"error": "ZIP file not found"}), 404

    # Run the processing in a background thread so the browser doesn't time out
    threading.Thread(target=process_master_zip, args=(zip_path,)).start()
    return jsonify({"message": "Ingestion started in background..."})

def process_master_zip(zip_path):
    print(f"📦 Processing ZIP: {zip_path}")
    today = date.today().isoformat()

    with zipfile.ZipFile(zip_path, 'r') as z:
        all_files = z.namelist()
        store_folders = sorted(list(set([f.split('/')[0] for f in all_files if '/' in f])))

        for store in store_folders:
            req = {'prods': f"{store}/products.csv", 'prices': f"{store}/prices.csv", 'stores': f"{store}/stores.csv"}
            if not all(f in all_files for f in req.values()): continue

            print(f"🏪 Syncing: {store}")
            try:
                df_p = pd.read_csv(z.open(req['prods']), dtype={'barcode': str})
                df_r = pd.read_csv(z.open(req['prices']))
                df_s = pd.read_csv(z.open(req['stores']))

                # Sync Products
                p_data = df_p[['barcode', 'name', 'brand', 'category']].dropna(subset=['barcode']).to_dict('records')
                bulk_upsert("master_products", p_data)

                # Merge and Sync Prices
                merged = df_r.merge(df_p[['product_id', 'barcode']], on='product_id')
                merged = merged.merge(df_s[['store_id', 'city']], on='store_id')
                
                prices = [{
                    "barcode": str(r['barcode']),
                    "store": f"{store.capitalize()} - {r['store_id']}",
                    "price_date": today,
                    "current_price": float(r['price']),
                    "is_on_sale": pd.notna(r['special_price'])
                } for _, r in merged.iterrows()]
                
                bulk_upsert("store_prices", prices)
            except Exception as e:
                print(f"❌ Error {store}: {e}")

@app.route("/")
def home():
    return "🚀 Katalog.ai Price Engine is Online (and Awake!)"

if __name__ == "__main__":
    # Render uses the PORT environment variable
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
