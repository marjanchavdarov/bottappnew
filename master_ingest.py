import os
import zipfile
import pandas as pd
import requests
import json
import time
import threading
import math
import gc
import re
from flask import Flask, render_template_string, request, Response, stream_with_context
from werkzeug.utils import secure_filename
from datetime import date
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp'
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024 

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

progress_status = {"msg": "System Ready", "percent": 0}

def db_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def sanitize_num(val):
    """Basic numeric cleaner for raw digits and European commas."""
    if pd.isna(val) or val == "" or val is None: return None
    try:
        s = str(val).replace(',', '.')
        match = re.search(r"[-+]?\d*\.\d+|\d+", s)
        if match:
            f = float(match.group())
            if math.isnan(f) or math.isinf(f): return None
            return f
        return None
    except: return None

def extract_from_name(row):
    """
    The Brain: Parses '125 g', '12 L', and '10/1' from the product name.
    """
    name_str = str(row.get('name', ''))
    
    # 1. Handle Multi-packs (e.g., '10/1', '2/1', '12/1')
    # This finds the number before the slash if it's a count
    multi_match = re.search(r"(\d+)/1\b", name_str)
    
    # 2. Handle Units (g, kg, l, ml)
    # This finds 125 in '125 g' or 12 in '12L'
    unit_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(g|l|ml|kg|kom)\b", name_str, re.IGNORECASE)
    
    if unit_match:
        val = sanitize_num(unit_match.group(1))
        unit = unit_match.group(2).lower()
        if val:
            # Normalize: Grams and Milliliters become 0.xxx (Standard Retail Unit)
            if unit in ['g', 'ml']:
                return val / 1000
            return val
            
    if multi_match:
        return sanitize_num(multi_match.group(1))

    # Fallback to existing quantity column if name yields nothing
    return sanitize_num(row.get('quantity'))

def bulk_upsert(table, data, batch_size=500):
    if not data: return True
    target = "barcode,store,price_date" if table == "store_prices" else "barcode"
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={target}"
    headers = db_headers()
    success = True
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            r = requests.post(url, headers=headers, json=batch, timeout=60)
            if r.status_code >= 400:
                print(f"❌ {table} Error: {r.text}")
                success = False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            success = False
    return success

def process_master_zip(zip_path):
    global progress_status
    today = date.today().isoformat()
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            all_files = z.namelist()
            store_folders = sorted(list(set([f.split('/')[0] for f in all_files if '/' in f])))
            total_stores = len(store_folders)

            for i, store in enumerate(store_folders):
                progress_status = {"msg": f"🧠 Extracting {store.upper()}...", "percent": int((i / total_stores) * 100)}
                req = {'p': f"{store}/products.csv", 'r': f"{store}/prices.csv", 's': f"{store}/stores.csv"}
                if not all(f in all_files for f in req.values()): continue

                df_p = pd.read_csv(z.open(req['p']), dtype={'barcode': str})
                df_p['barcode'] = df_p['barcode'].astype(str).apply(lambda x: x.split('.')[0].strip())
                
                # APPLY THE SMART EXTRACTION
                df_p['quantity'] = df_p.apply(extract_from_name, axis=1)

                p_cols = ['barcode', 'name', 'brand', 'category', 'unit', 'quantity']
                existing_p = [c for c in p_cols if c in df_p.columns]
                master_data = df_p[existing_p].dropna(subset=['barcode']).drop_duplicates('barcode').to_dict('records')
                
                if bulk_upsert("master_products", master_data):
                    del master_data
                    gc.collect()

                    df_r = pd.read_csv(z.open(req['r']))
                    df_s = pd.read_csv(z.open(req['s']))
                    merged = df_r.merge(df_p[['product_id', 'barcode']], on='product_id')
                    merged = merged.merge(df_s[['store_id', 'city']], on='store_id')
                    
                    price_records = []
                    for _, row in merged.iterrows():
                        curr_p = sanitize_num(row['price'])
                        price_records.append({
                            "barcode":       str(row['barcode']).split('.')[0].strip(),
                            "store":         f"{store.capitalize()} - {row['store_id']} ({row['city']})",
                            "price_date":    today,
                            "current_price": curr_p,
                            "regular_price": sanitize_num(row['anchor_price']) if pd.notna(row['anchor_price']) else curr_p,
                            "sale_price":    sanitize_num(row['special_price']),
                            "is_on_sale":    pd.notna(row['special_price'])
                        })
                    bulk_upsert("store_prices", price_records)
                del df_p
                gc.collect()
            progress_status = {"msg": "✅ Intelligence Sync Complete!", "percent": 100}
    except Exception as e:
        progress_status = {"msg": f"❌ Error: {str(e)}", "percent": 0}
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)
        gc.collect()

# --- FLASK ROUTES REMAIN THE SAME ---
HTML_PAGE = """
<!DOCTYPE html><html><head><title>Katalog AI Sync</title>
<style>
    body { font-family: sans-serif; background: #edf2f7; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
    .card { background: white; padding: 2rem; border-radius: 20px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); width: 400px; text-align: center; }
    .progress-box { background: #cbd5e0; border-radius: 10px; height: 12px; margin: 20px 0; overflow: hidden; }
    #bar { background: #38a169; width: 0%; height: 100%; transition: width 0.3s; }
    button { background: #2b6cb0; color: white; border: none; padding: 12px; width: 100%; border-radius: 10px; cursor: pointer; font-weight: bold; }
</style></head>
<body><div class="card">
    <h2>🤖 AI Ingest Active</h2>
    <form id="uForm"><input type="file" id="fIn" accept=".zip"><br><br><button type="submit" id="sBtn">Sync Now</button></form>
    <div class="progress-box"><div id="bar"></div></div>
    <div id="status">Ready</div>
</div>
<script>
    const form = document.getElementById('uForm'), bar = document.getElementById('bar'), status = document.getElementById('status'), btn = document.getElementById('sBtn');
    form.onsubmit = async (e) => {
        e.preventDefault(); const file = document.getElementById('fIn').files[0]; if(!file) return;
        btn.disabled = true; status.innerText = "Processing...";
        const formData = new FormData(); formData.append('file', file);
        const uploadResp = await fetch('/upload', { method: 'POST', body: formData });
        if (uploadResp.ok) {
            const ev = new EventSource('/stream');
            ev.onmessage = (e) => {
                const data = JSON.parse(e.data); bar.style.width = data.percent + '%'; status.innerText = data.msg;
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
