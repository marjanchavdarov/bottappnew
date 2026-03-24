import os, zipfile, pandas as pd, requests, json, time, threading, math, gc, re
from flask import Flask, render_template_string, request, jsonify
from werkzeug.utils import secure_filename
from datetime import date
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp'
# Lowering this slightly to prevent massive files from even starting if they'll obviously fail
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

state = {"msg": "System Ready", "percent": 0, "logs": ["Memory-optimized mode active."]}

def add_log(message):
    global state
    ts = time.strftime("%H:%M:%S")
    msg = f"[{ts}] {message}"
    state["logs"].append(msg)
    state["msg"] = message
    if len(state["logs"]) > 100: state["logs"].pop(0)

def sanitize_num(val):
    if pd.isna(val) or val == "" or val is None: return None
    try:
        s = str(val).replace(',', '.')
        match = re.search(r"[-+]?\d*\.\d+|\d+", s)
        if match:
            f = float(match.group())
            return None if math.isnan(f) or math.isinf(f) else f
    except: pass
    return None

def extract_from_name(name_val, quant_val):
    name_str = str(name_val or '')
    multi = re.search(r"(\d+)/1\b", name_str)
    unit_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(g|l|ml|kg|kom)\b", name_str, re.IGNORECASE)
    if unit_match:
        val = sanitize_num(unit_match.group(1))
        unit = unit_match.group(2).lower()
        if val:
            if unit in ['g', 'ml']: return val / 1000
            return val
    if multi: return sanitize_num(multi.group(1))
    return sanitize_num(quant_val)

def bulk_upsert(table, data):
    if not data: return True
    df = pd.DataFrame(data).drop_duplicates(subset=['barcode', 'store', 'price_date'] if table == "store_prices" else ['barcode'])
    clean = json.loads(json.dumps(df.to_dict('records'), default=lambda o: None if isinstance(o, float) and (math.isnan(o) or math.isinf(o)) else o))
    
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={'barcode,store,price_date' if table == 'store_prices' else 'barcode'}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
    
    try:
        r = requests.post(url, headers=headers, json=clean, timeout=60)
        return r.status_code < 400
    except: return False

def process_master_zip(zip_path):
    global state
    today = date.today().isoformat()
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            folders = sorted(list(set([f.split('/')[0] for f in z.namelist() if '/' in f])))
            
            for i, store in enumerate(folders):
                state["percent"] = int((i / len(folders)) * 100)
                add_log(f"🛠️ Memory-Safe Sync: {store.upper()}")
                
                # 1. PROCESS PRODUCTS IN CHUNKS
                try:
                    p_iter = pd.read_csv(z.open(f"{store}/products.csv"), dtype={'barcode': str}, chunksize=1000)
                    for chunk in p_iter:
                        chunk['barcode'] = chunk['barcode'].astype(str).str.split('.').str[0].str.strip()
                        chunk['quantity'] = chunk.apply(lambda r: extract_from_name(r.get('name'), r.get('quantity')), axis=1)
                        p_data = chunk[['barcode', 'name', 'brand', 'category', 'unit', 'quantity']].dropna(subset=['barcode']).to_dict('records')
                        bulk_upsert("master_products", p_data)
                        del chunk; gc.collect()
                except: pass

                # 2. PROCESS PRICES (Need to join products for barcode)
                # To save RAM, we only keep the mapping of product_id -> barcode
                df_p = pd.read_csv(z.open(f"{store}/products.csv"), usecols=['product_id', 'barcode'], dtype={'barcode': str})
                df_p['barcode'] = df_p['barcode'].astype(str).str.split('.').str[0].str.strip()
                df_s = pd.read_csv(z.open(f"{store}/stores.csv"), usecols=['store_id', 'city'])
                
                r_iter = pd.read_csv(z.open(f"{store}/prices.csv"), chunksize=1000)
                for r_chunk in r_iter:
                    merged = r_chunk.merge(df_p, on='product_id').merge(df_s, on='store_id')
                    recs = []
                    for _, row in merged.iterrows():
                        p = sanitize_num(row['price'])
                        if p and pd.notna(row['barcode']):
                            recs.append({
                                "barcode": str(row['barcode']),
                                "store": f"{store.capitalize()} - {row['city']}",
                                "price_date": today,
                                "current_price": p,
                                "regular_price": sanitize_num(row.get('anchor_price')) or p
                            })
                    bulk_upsert("store_prices", recs)
                    del r_chunk, merged, recs; gc.collect()
                
                del df_p, df_s; gc.collect()
                add_log(f"✅ {store} Complete.")

            state["percent"] = 100
            add_log("🏁 FINISHED.")
    except Exception as e:
        add_log(f"🔥 Error: {str(e)}")
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)

# --- REUSE THE SAME FLASK ROUTES AND HTML FROM PREVIOUS VERSION ---
@app.route('/')
def index():
    return render_template_string("""
<!DOCTYPE html><html><head><title>Katalog Console</title>
<style>
    body { font-family: sans-serif; background: #0f172a; color: #e2e8f0; padding: 20px; }
    .card { max-width: 800px; margin: auto; background: #1e293b; padding: 25px; border-radius: 12px; }
    #console { background: #000; height: 350px; overflow-y: auto; padding: 10px; border-radius: 8px; font-size: 13px; color: #10b981; border: 1px solid #334155; margin-top: 15px; user-select: text; }
    .bar-bg { background: #334155; height: 10px; border-radius: 5px; margin: 15px 0; }
    #bar { background: #3b82f6; width: 0%; height: 100%; transition: width 0.5s; }
    button { background: #3b82f6; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; }
</style></head>
<body><div class="card">
    <h2>🚀 Low-Memory Data Engine</h2>
    <input type="file" id="f"><button id="b" onclick="runSync()">Start Ingest</button>
    <div class="bar-bg"><div id="bar"></div></div>
    <div id="st">Ready</div>
    <div id="console"></div>
</div>
<script>
    function runSync() {
        var file = document.getElementById('f').files[0]; if(!file) return;
        document.getElementById('b').disabled = true;
        var fd = new FormData(); fd.append('file', file);
        document.getElementById('st').innerText = "Uploading...";
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/upload', true);
        xhr.onload = function() { if(xhr.status === 200) startPolling(); };
        xhr.send(fd);
    }
    function startPolling() {
        var poller = setInterval(function() {
            var sxhr = new XMLHttpRequest();
            sxhr.open('GET', '/status', true);
            sxhr.onload = function() {
                var d = JSON.parse(sxhr.responseText);
                document.getElementById('bar').style.width = d.percent + '%';
                document.getElementById('st').innerText = d.percent + '% - ' + d.msg;
                document.getElementById('console').innerHTML = d.logs.map(function(l){ return '<div>'+l+'</div>'; }).join('');
                if(d.percent >= 100) clearInterval(poller);
            };
            sxhr.send();
        }, 2000);
    }
</script></body></html>
""")

@app.route('/status')
def get_status(): return jsonify(state)

@app.route('/upload', methods=['POST'])
def upload():
    global state
    state = {"msg": "Processing...", "percent": 0, "logs": ["Starting Low-Memory Sync..."]}
    f = request.files['file']
    p = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename))
    f.save(p)
    threading.Thread(target=process_master_zip, args=(p,)).start()
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
