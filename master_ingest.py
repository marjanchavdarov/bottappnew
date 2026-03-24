import os
import zipfile
import pandas as pd
import requests
import json
import time
import threading
import math
import gc 
from flask import Flask, render_template_string, request, Response, stream_with_context
from werkzeug.utils import secure_filename
from datetime import date
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp'
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024 

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

progress_status = {"msg": "Ready to sync", "percent": 0}

# 1. HELPERS
def db_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def clean_float(val):
    """Fixes 'Out of range float' by converting NaN/Inf to None (JSON null)"""
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except:
        return None

def bulk_upsert(table, data, batch_size=1000):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            r = requests.post(url, headers=db_headers(), json=batch, timeout=60)
            if r.status_code >= 400:
                print(f"Error in {table}: {r.text}")
        except Exception as e:
            print(f"Batch failed: {e}")

# 2. MAIN PROCESSOR


def process_master_zip(zip_path):
    global progress_status
    today = date.today().isoformat()
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            all_files = z.namelist()
            store_folders = sorted(list(set([f.split('/')[0] for f in all_files if '/' in f])))
            total_stores = len(store_folders)

            for i, store in enumerate(store_folders):
                progress_status = {
                    "msg": f"Ingesting {store.upper()} ({i+1}/{total_stores})",
                    "percent": int((i / total_stores) * 100)
                }

                req = {'p': f"{store}/products.csv", 'r': f"{store}/prices.csv", 's': f"{store}/stores.csv"}
                if not all(f in all_files for f in req.values()): continue

                # 1. Load and Sync Products (The "Master" table)
                df_p = pd.read_csv(z.open(req['p']), dtype={'barcode': str})
                p_cols = ['barcode', 'name', 'brand', 'category', 'unit', 'quantity']
                existing_p = [c for c in p_cols if c in df_p.columns]
                
                # Clean barcodes to remove .0 and whitespace
                df_p['barcode'] = df_p['barcode'].astype(str).apply(lambda x: x.split('.')[0].strip())
                
                master_data = df_p[existing_p].dropna(subset=['barcode']).drop_duplicates('barcode').to_dict('records')
                bulk_upsert("master_products", master_data)
                del master_data

                # --- CRITICAL PAUSE ---
                # This gives Supabase time to index the barcodes before we send prices
                time.sleep(2) 

                # 2. Load Prices and Stores
                df_r = pd.read_csv(z.open(req['r']))
                df_s = pd.read_csv(z.open(req['s']))

                # 3. Merge Data
                merged = df_r.merge(df_p[['product_id', 'barcode']], on='product_id')
                merged = merged.merge(df_s[['store_id', 'city']], on='store_id')
                
                # Free memory from raw dataframes
                del df_p, df_r, df_s
                gc.collect()

                price_records = []
                for _, row in merged.iterrows():
                    curr_p = clean_float(row['price'])
                    # Ensure price barcode matches the cleaned product barcode
                    clean_bc = str(row['barcode']).split('.')[0].strip()
                    
                    price_records.append({
                        "barcode":       clean_bc,
                        "store":         f"{store.capitalize()} - {row['store_id']} ({row['city']})",
                        "price_date":    today,
                        "current_price": curr_p,
                        "regular_price": clean_float(row['anchor_price']) if pd.notna(row['anchor_price']) else curr_p,
                        "sale_price":    clean_float(row['special_price']),
                        "is_on_sale":    pd.notna(row['special_price'])
                    })
                
                # 4. Upload Prices (The "Timeline" table)
                bulk_upsert("store_prices", price_records)
                
                # Clean up memory after each store folder
                del merged, price_records
                gc.collect()

            progress_status = {"msg": "✅ Sync Complete!", "percent": 100}

    except Exception as e:
        progress_status = {"msg": f"❌ Error: {str(e)}", "percent": 0}
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)
        gc.collect()
        
# 3. ROUTES
HTML_PAGE = """
<!DOCTYPE html><html><head><title>Katalog Ingest</title>
<style>
    body { font-family: sans-serif; background: #f7fafc; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
    .card { background: white; padding: 2rem; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); width: 400px; text-align: center; }
    .progress-box { background: #edf2f7; border-radius: 10px; height: 12px; margin: 20px 0; overflow: hidden; }
    #bar { background: #48bb78; width: 0%; height: 100%; transition: width 0.4s; }
    input { margin-bottom: 20px; }
    button { background: #4299e1; color: white; border: none; padding: 12px; width: 100%; border-radius: 8px; cursor: pointer; font-weight: bold; }
</style></head>
<body><div class="card">
    <h2>🚀 Katalog.ai Ingest</h2>
    <form id="uForm"><input type="file" id="fIn" accept=".zip"><button type="submit" id="sBtn">Start Sync</button></form>
    <div class="progress-box"><div id="bar"></div></div>
    <div id="status">Ready</div>
</div>
<script>
    const form = document.getElementById('uForm');
    const bar = document.getElementById('bar');
    const status = document.getElementById('status');
    const btn = document.getElementById('sBtn');

    form.onsubmit = async (e) => {
        e.preventDefault();
        const file = document.getElementById('fIn').files[0];
        if(!file) return;
        btn.disabled = true; status.innerText = "Uploading ZIP...";
        
        const formData = new FormData();
        formData.append('file', file);
        const uploadResp = await fetch('/upload', { method: 'POST', body: formData });
        
        if (uploadResp.ok) {
            const ev = new EventSource('/stream');
            ev.onmessage = (e) => {
                const data = JSON.parse(e.data);
                bar.style.width = data.percent + '%';
                status.innerText = data.msg;
                if(data.percent >= 100) { ev.close(); btn.disabled = false; }
            };
        }
    };
</script></body></html>
"""

@app.route('/')
def index(): return render_template_string(HTML_PAGE)

@app.route('/stream')
def stream():
    def gen():
        while True:
            yield f"data: {json.dumps(progress_status)}\\n\\n"
            if progress_status['percent'] >= 100: break
            time.sleep(1)
    return Response(stream_with_context(gen()), mimetype='text/event-stream')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
    file.save(path)
    threading.Thread(target=process_master_zip, args=(path,)).start()
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
