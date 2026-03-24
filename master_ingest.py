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

def db_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def clean_float(val):
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f): return None
        return f
    except: return None

def bulk_upsert(table, data, batch_size=1000):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    success = True
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            r = requests.post(url, headers=db_headers(), json=batch, timeout=60)
            if r.status_code >= 400:
                print(f"❌ Error in {table}: {r.text}")
                success = False
        except Exception as e:
            print(f"❌ Request failed: {e}")
            success = False
    return success

# --- THE ORDERED PROCESSOR ---
def process_master_zip(zip_path):
    global progress_status
    today = date.today().isoformat()
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            all_files = z.namelist()
            store_folders = sorted(list(set([f.split('/')[0] for f in all_files if '/' in f])))
            total_stores = len(store_folders)

            for i, store in enumerate(store_folders):
                # UI Update
                progress_status = {
                    "msg": f"⏳ Phase 1: Syncing {store.upper()} Barcodes...",
                    "percent": int((i / total_stores) * 100)
                }

                req = {'p': f"{store}/products.csv", 'r': f"{store}/prices.csv", 's': f"{store}/stores.csv"}
                if not all(f in all_files for f in req.values()): continue

                # 1. SYNC PRODUCTS FIRST (The Parent)
                df_p = pd.read_csv(z.open(req['p']), dtype={'barcode': str})
                df_p['barcode'] = df_p['barcode'].astype(str).apply(lambda x: x.split('.')[0].strip())
                
                p_cols = ['barcode', 'name', 'brand', 'category', 'unit', 'quantity']
                existing_p = [c for c in p_cols if c in df_p.columns]
                master_data = df_p[existing_p].dropna(subset=['barcode']).drop_duplicates('barcode').to_dict('records')
                
                # STOP AND WAIT: Don't move to prices until products are confirmed
                if bulk_upsert("master_products", master_data):
                    del master_data
                    
                    # 2. SYNC PRICES SECOND (The Child)
                    progress_status["msg"] = f"🚀 Phase 2: Uploading {store.upper()} Prices..."
                    df_r = pd.read_csv(z.open(req['r']))
                    df_s = pd.read_csv(z.open(req['s']))

                    merged = df_r.merge(df_p[['product_id', 'barcode']], on='product_id')
                    merged = merged.merge(df_s[['store_id', 'city']], on='store_id')
                    
                    del df_p, df_r, df_s
                    gc.collect()

                    price_records = []
                    for _, row in merged.iterrows():
                        curr_p = clean_float(row['price'])
                        price_records.append({
                            "barcode":       str(row['barcode']).split('.')[0].strip(),
                            "store":         f"{store.capitalize()} - {row['store_id']} ({row['city']})",
                            "price_date":    today,
                            "current_price": curr_p,
                            "regular_price": clean_float(row['anchor_price']) if pd.notna(row['anchor_price']) else curr_p,
                            "sale_price":    clean_float(row['special_price']),
                            "is_on_sale":    pd.notna(row['special_price'])
                        })
                    
                    bulk_upsert("store_prices", price_records)
                    del merged, price_records
                
                gc.collect()

            progress_status = {"msg": "✅ Database Updated Successfully!", "percent": 100}

    except Exception as e:
        progress_status = {"msg": f"❌ Error: {str(e)}", "percent": 0}
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)
        gc.collect()

# --- WEB UI (As provided before) ---
HTML_PAGE = """
<!DOCTYPE html><html><head><title>Katalog Admin</title>
<style>
    body { font-family: sans-serif; background: #f0f4f8; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
    .card { background: white; padding: 2rem; border-radius: 20px; box-shadow: 0 10px 25px rgba(0,0,0,0.1); width: 420px; text-align: center; }
    .progress-box { background: #e2e8f0; border-radius: 10px; height: 15px; margin: 25px 0; overflow: hidden; }
    #bar { background: #4299e1; width: 0%; height: 100%; transition: width 0.3s; }
    #status { font-weight: bold; color: #4a5568; }
    button { background: #48bb78; color: white; border: none; padding: 15px; width: 100%; border-radius: 10px; cursor: pointer; font-size: 1.1rem; }
    button:disabled { background: #cbd5e0; }
</style></head>
<body><div class="card">
    <h2>📦 Katalog Data Sync</h2>
    <form id="f"><input type="file" id="i" accept=".zip"><br><br><button type="submit" id="b">Update Database</button></form>
    <div class="progress-box"><div id="bar"></div></div>
    <div id="status">Ready</div>
</div>
<script>
    const f=document.getElementById('f'),b=document.getElementById('bar'),s=document.getElementById('status'),btn=document.getElementById('b');
    f.onsubmit=async(e)=>{
        e.preventDefault(); const file=document.getElementById('i').files[0]; if(!file)return;
        btn.disabled=true; s.innerText="Uploading ZIP...";
        const fd=new FormData(); fd.append('file',file);
        const r=await fetch('/upload',{method:'POST',body:fd});
        if(r.ok){
            const ev=new EventSource('/stream');
            ev.onmessage=(e)=>{
                const d=JSON.parse(e.data); b.style.width=d.percent+'%'; s.innerText=d.msg;
                if(d.percent>=100){ev.close(); btn.disabled=false;}
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
