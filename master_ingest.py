import os, zipfile, pandas as pd, requests, json, time, threading, math, gc, re
from flask import Flask, render_template_string, request, jsonify
from werkzeug.utils import secure_filename
from datetime import date
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/tmp'
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

state = {"msg": "System Ready", "percent": 0, "logs": ["Console Initialized..."]}

def add_log(message):
    global state
    ts = time.strftime("%H:%M:%S")
    msg = f"[{ts}] {message}"
    print(msg)
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

def bulk_upsert(table, data, batch_size=400):
    if not data: return True
    df = pd.DataFrame(data)
    subset = ['barcode', 'store', 'price_date'] if table == "store_prices" else ['barcode']
    df = df.drop_duplicates(subset=subset)
    clean = json.loads(json.dumps(df.to_dict('records'), default=lambda o: None if isinstance(o, float) and (math.isnan(o) or math.isinf(o)) else o))
    target = "barcode,store,price_date" if table == "store_prices" else "barcode"
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={target}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
    for i in range(0, len(clean), batch_size):
        batch = clean[i:i + batch_size]
        try:
            r = requests.post(url, headers=headers, json=batch, timeout=60)
            if r.status_code >= 400:
                add_log(f"❌ {table} Error: {r.text[:100]}")
                return False
        except Exception as e:
            add_log(f"❌ Connection Error: {str(e)}")
            return False
    return True

def process_master_zip(zip_path):
    global state
    today = date.today().isoformat()
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            all_files = z.namelist()
            folders = sorted(list(set([f.split('/')[0] for f in all_files if '/' in f])))
            add_log(f"📦 Found {len(folders)} store folders.")
            for i, store in enumerate(folders):
                state["percent"] = int((i / len(folders)) * 100)
                add_log(f"📂 Processing: {store.upper()}")
                paths = {'p': f"{store}/products.csv", 'r': f"{store}/prices.csv", 's': f"{store}/stores.csv"}
                if not all(f in all_files for f in paths.values()): continue
                df_p = pd.read_csv(z.open(paths['p']), dtype={'barcode': str})
                df_p['barcode'] = df_p['barcode'].astype(str).str.split('.').str[0].str.strip()
                p_data = df_p[['barcode', 'name']].dropna(subset=['barcode']).to_dict('records')
                if bulk_upsert("master_products", p_data):
                    add_log(f"✅ {store}: Products synced.")
                    df_r = pd.read_csv(z.open(paths['r']))
                    df_s = pd.read_csv(z.open(paths['s']))
                    merged = df_r.merge(df_p[['product_id', 'barcode']], on='product_id').merge(df_s[['store_id', 'city']], on='store_id')
                    price_recs = []
                    for _, row in merged.iterrows():
                        p = sanitize_num(row['price'])
                        if p and pd.notna(row['barcode']):
                            price_recs.append({
                                "barcode": str(row['barcode']).split('.')[0].strip(),
                                "store": f"{store.capitalize()} - {row['city']}",
                                "price_date": today,
                                "current_price": p,
                                "regular_price": sanitize_num(row['anchor_price']) or p
                            })
                    if bulk_upsert("store_prices", price_recs):
                        add_log(f"💰 {store}: Prices synced.")
                gc.collect()
            state["percent"] = 100
            add_log("🏁 FINISHED.")
    except Exception as e:
        add_log(f"🔥 CRITICAL ERROR: {str(e)}")
    finally:
        if os.path.exists(zip_path): os.remove(zip_path)

@app.route('/')
def index():
    return render_template_string("""
<!DOCTYPE html><html><head><title>Katalog Console</title>
<style>
    body { font-family: sans-serif; background: #0f172a; color: #e2e8f0; padding: 20px; }
    .card { max-width: 800px; margin: auto; background: #1e293b; padding: 25px; border-radius: 12px; }
    #console { background: #000; height: 350px; overflow-y: auto; padding: 10px; border-radius: 8px; font-size: 13px; color: #10b981; border: 1px solid #334155; margin-top: 15px; }
    .bar-bg { background: #334155; height: 10px; border-radius: 5px; margin: 15px 0; overflow: hidden; }
    #bar { background: #3b82f6; width: 0%; height: 100%; }
    button { background: #3b82f6; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; }
</style></head>
<body><div class="card">
    <h2>🚀 Katalog Data Engine</h2>
    <input type="file" id="f"><button id="b" onclick="runSync()">Start Sync</button>
    <div class="bar-bg"><div id="bar"></div></div>
    <div id="st">Status: Ready</div>
    <div id="console"></div>
</div>
<script>
    function runSync() {
        var file = document.getElementById('f').files[0];
        if (!file) { alert('Please select a ZIP file first!'); return; }
        
        var btn = document.getElementById('b');
        btn.disabled = true;
        
        var fd = new FormData();
        fd.append('file', file);
        
        document.getElementById('st').innerText = "Uploading ZIP...";
        
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/upload', true);
        xhr.onload = function() {
            if (xhr.status === 200) {
                document.getElementById('console').innerHTML = '<div>> ZIP Uploaded. Starting Sync...</div>';
                startStatusPolling();
            } else {
                alert('Upload Failed: ' + xhr.status);
                btn.disabled = false;
            }
        };
        xhr.send(fd);
    }

    function startStatusPolling() {
        var poller = setInterval(function() {
            var sxhr = new XMLHttpRequest();
            sxhr.open('GET', '/status', true);
            sxhr.onload = function() {
                if (sxhr.status === 200) {
                    var data = JSON.parse(sxhr.responseText);
                    document.getElementById('bar').style.width = data.percent + '%';
                    document.getElementById('st').innerText = 'Syncing: ' + data.percent + '%';
                    var c = document.getElementById('console');
                    c.innerHTML = data.logs.map(function(l){ return '<div>'+l+'</div>'; }).join('');
                    c.scrollTop = c.scrollHeight;
                    if (data.percent >= 100) { clearInterval(poller); document.getElementById('b').disabled = false; }
                }
            };
            sxhr.send();
        }, 1500);
    }
</script></body></html>
""")

@app.route('/status')
def get_status(): return jsonify(state)

@app.route('/upload', methods=['POST'])
def upload():
    global state
    state = {"msg": "Starting Process...", "percent": 0, "logs": ["Starting new upload..."]}
    file = request.files['file']
    path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
    file.save(path)
    threading.Thread(target=process_master_zip, args=(path,)).start()
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
