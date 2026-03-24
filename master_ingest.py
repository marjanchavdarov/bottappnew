import os
import zipfile
import pandas as pd
import requests
import json
import time
import threading
from flask import Flask, render_template_string, request, Response, stream_with_context
from werkzeug.utils import secure_filename
from datetime import date
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp'
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024 # 150MB Limit

# --- CONFIG ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Global state to track progress
progress_status = {"msg": "Ready to sync", "percent": 0}

# 1. DATABASE HELPERS
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
        try:
            requests.post(url, headers=db_headers(), json=batch, timeout=30)
        except Exception as e:
            print(f"Batch error: {e}")

# 2. THE BACKGROUND PROCESSOR
def process_master_zip(zip_path):
    global progress_status
    today = date.today().isoformat()
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            all_files = z.namelist()
            # Find folders that have the required CSV trio
            store_folders = sorted(list(set([f.split('/')[0] for f in all_files if '/' in f])))
            total_stores = len(store_folders)

            if total_stores == 0:
                progress_status = {"msg": "❌ Error: No store folders found in ZIP", "percent": 0}
                return

            for i, store in enumerate(store_folders):
                # Update Progress Bar
                progress_status = {
                    "msg": f"Ingesting {store.upper()} ({i+1}/{total_stores})",
                    "percent": int((i / total_stores) * 100)
                }

                req = {'p': f"{store}/products.csv", 'r': f"{store}/prices.csv", 's': f"{store}/stores.csv"}
                if not all(f in all_files for f in req.values()):
                    continue

                # Load and Clean
                df_p = pd.read_csv(z.open(req['p']), dtype={'barcode': str})
                df_r = pd.read_csv(z.open(req['r']))
                df_s = pd.read_csv(z.open(req['s']))

                # Sync Products (Identity Layer)
                p_cols = ['barcode', 'name', 'brand', 'category', 'unit', 'quantity']
                existing_p = [c for c in p_cols if c in df_p.columns]
                master_data = df_p[existing_p].dropna(subset=['barcode']).drop_duplicates('barcode').to_dict('records')
                bulk_upsert("master_products", master_data)

                # Merge and Sync Prices (Timeline Layer)
                merged = df_r.merge(df_p[['product_id', 'barcode']], on='product_id')
                merged = merged.merge(df_s[['store_id', 'city']], on='store_id')
                
                price_records = []
                for _, row in merged.iterrows():
                    price_records.append({
                        "barcode":       str(row['barcode']),
                        "store":         f"{store.capitalize()} - {row['store_id']} ({row['city']})",
                        "price_date":    today,
                        "current_price": float(row['price']),
                        "regular_price": float(row['anchor_price']) if pd.notna(row['anchor_price']) else float(row['price']),
                        "sale_price":    float(row['special_price']) if pd.notna(row['special_price']) else None,
                        "is_on_sale":    pd.notna(row['special_price'])
                    })
                
                bulk_upsert("store_prices", price_records)

            progress_status = {"msg": "✅ Sync Complete! Everything is up to date.", "percent": 100}

    except Exception as e:
        progress_status = {"msg": f"❌ Error: {str(e)}", "percent": 0}
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)

# 3. WEB INTERFACE
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Katalog.ai | Admin</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica; background: #f7fafc; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .card { background: white; padding: 2.5rem; border-radius: 20px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); width: 450px; }
        h1 { color: #2d3748; text-align: center; margin-bottom: 2rem; font-size: 1.5rem; }
        .progress-box { background: #edf2f7; border-radius: 12px; height: 12px; width: 100%; margin: 25px 0; overflow: hidden; }
        #bar { background: #48bb78; width: 0%; height: 100%; transition: width 0.4s cubic-bezier(0.4, 0, 0.2, 1); }
        #status { text-align: center; color: #718096; font-size: 0.95rem; font-weight: 500; min-height: 1.2rem; }
        input[type="file"] { border: 2px dashed #e2e8f0; padding: 20px; width: 100%; border-radius: 10px; margin-bottom: 20px; cursor: pointer; box-sizing: border-box; }
        button { background: #4299e1; color: white; border: none; padding: 14px; width: 100%; border-radius: 10px; cursor: pointer; font-size: 1rem; font-weight: 600; box-shadow: 0 4px 6px rgba(66, 153, 225, 0.2); }
        button:disabled { background: #cbd5e0; cursor: not-allowed; }
    </style>
</head>
<body>
    <div class="card">
        <h1>📦 Katalog Ingestor</h1>
        <form id="uForm">
            <input type="file" id="fIn" accept=".zip">
            <button type="submit" id="sBtn">Upload & Sync Data</button>
        </form>
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

            btn.disabled = true;
            status.innerText = "Uploading ZIP to server...";
            
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
    </script>
</body>
</html>
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
