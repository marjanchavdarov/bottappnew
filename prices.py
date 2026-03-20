"""
prices.py — katalog-prices web service
Handles manual uploads AND auto-downloads from store websites.
Supports CSV, XML, and ZIP files containing multiple CSVs.
"""

import os
import io
import re
import threading
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import date
from flask import Flask, jsonify, request, render_template_string
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

UPLOAD_PASSWORD      = os.environ.get("UPLOAD_PASSWORD", "katalog2026")
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY         = os.environ.get("SUPABASE_KEY", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", SUPABASE_KEY)

# ─── Auto-download URLs per store ─────────────────────────────────────────────
# Each entry: (url, file_type)
# file_type: 'csv', 'xml', 'zip_csv', 'zip_xml', 'index' (parse index page for links)
STORE_SOURCES = {
    "lidl":     ("https://tvrtka.lidl.hr/fileadmin/user_upload/HR_Cjenik.zip", "zip_csv"),
    "spar":     ("https://spar.hr/datoteke_cjenici/index.html",                "index"),
    "kaufland": ("https://www.kaufland.hr/popis-mpc.html",                     "index"),
    "plodine":  ("https://www.plodine.hr/info-o-cijenama",                     "index"),
    "tommy":    ("https://www.tommy.hr/fileadmin/user_upload/cjenik.zip",      "zip_csv"),
}

job = {
    "running": False, "status": "idle", "processed": 0,
    "total": 0, "current_file": "", "errors": [], "log": [],
}

def log(msg):
    print(msg)
    job["log"].append(msg)
    if len(job["log"]) > 1000:
        job["log"] = job["log"][-1000:]

# ─── Supabase ─────────────────────────────────────────────────────────────────
def db_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }

def upsert(table, records, batch_size=500, conflict=None):
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        if conflict:
            url += f"?on_conflict={conflict}"
        resp = requests.post(url, headers=db_headers(), json=batch, timeout=30)
        if resp.status_code not in (200, 201):
            log(f"  ❌ {table} error: {resp.status_code} {resp.text[:200]}")
        else:
            total += len(batch)
    return total

# ─── Column hints (works across all stores) ───────────────────────────────────
COLUMN_HINTS = [
    ("name",             ["naziv"]),
    ("brand",            ["marka", "brand"]),
    ("quantity",         ["neto", "kolici", "koli", "grama"]),
    ("unit",             ["jedinica mjere", "jedinica"]),
    ("regular_price",    ["maloprodajna", "maloprod", "mpc (eur)", "mpc(eur)", "mpc eur", "mpc"]),
    ("sale_price",       ["posebnog oblika", "posebno", "akcij", "akc", "sale"]),
    ("lowest_30d_price", ["30 dan", "30dana", "30 dana", "najni"]),
    ("anchor_price",     ["sidrena", "anchor"]),
    ("barcode",          ["barkod", "barcode", "ean"]),
    ("category",         ["kategorij", "category", "grupe"]),
]

def fuzzy_rename(df):
    cols_lower = {c: c.lower() for c in df.columns}
    rename_map = {}
    already_mapped = set()
    for std_name, hints in COLUMN_HINTS:
        for col, col_l in cols_lower.items():
            if col in rename_map or std_name in already_mapped:
                continue
            if any(h in col_l for h in hints):
                rename_map[col] = std_name
                already_mapped.add(std_name)
                break
    df = df.rename(columns=rename_map)
    found = sorted(already_mapped)
    missing = [h[0] for h in COLUMN_HINTS if h[0] not in already_mapped]
    log(f"  Columns: {found}" + (f" | missing: {missing}" if missing else ""))
    return df

# ─── Extract location from filename ───────────────────────────────────────────
def location_from_filename(filename):
    """
    Extract a human-readable location from store filenames.
    Examples:
      SupermarketTrg-Grada-Vukovara-8-Velika-Gorica-10410-... → Velika Gorica
      SUPERMARKET_ZAGREBAČKA_18_53000_GOSPIĆ_...              → Gospić
      supermarket_split_licka_ulica_...                       → Split Lička
    """
    name = os.path.splitext(os.path.basename(filename))[0]
    # Remove date/time patterns
    name = re.sub(r'[\d]{4}[-_][\d]{2}[-_][\d]{2}.*', '', name)
    name = re.sub(r'[\d]{5,}', '', name)       # remove long numbers (postal codes etc)
    name = re.sub(r'[-_]+', ' ', name)          # replace separators with spaces
    name = re.sub(r'\s+', ' ', name).strip()
    # Capitalize words, take last 3 meaningful words as location
    words = [w.capitalize() for w in name.split() if len(w) > 2]
    return ' '.join(words[-3:]) if words else name[:50]

# ─── Parse CSV ────────────────────────────────────────────────────────────────
def parse_csv(filepath_or_bytes, store, filename=""):
    df = None
    is_bytes = isinstance(filepath_or_bytes, (bytes, io.BytesIO))

    for encoding in ["utf-8", "utf-8-sig", "cp1250", "latin-1"]:
        try:
            src = io.BytesIO(filepath_or_bytes) if isinstance(filepath_or_bytes, bytes) else \
                  filepath_or_bytes if is_bytes else filepath_or_bytes
            df = pd.read_csv(
                src if is_bytes else filepath_or_bytes,
                sep=None, engine="python",
                encoding=encoding, dtype=str, skipinitialspace=True,
            )
            log(f"  Encoding: {encoding} — {len(df)} rows")
            break
        except Exception:
            if is_bytes:
                filepath_or_bytes = io.BytesIO(filepath_or_bytes if isinstance(filepath_or_bytes, bytes) else filepath_or_bytes.getvalue())
            continue

    if df is None:
        raise ValueError("Could not open CSV with any known encoding")

    df.columns = [c.strip() for c in df.columns]
    df = fuzzy_rename(df)

    for col in ["regular_price", "sale_price", "lowest_30d_price", "anchor_price"]:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.strip()
                    .str.replace(",", ".", regex=False)
                    .str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            )

    if "barcode" not in df.columns:
        raise ValueError("No barcode column found")

    df["barcode"] = df["barcode"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
    df = df[df["barcode"].notna() & (df["barcode"] != "") & (df["barcode"] != "nan")]
    df["current_price"] = df["sale_price"].combine_first(df["regular_price"])
    df["is_on_sale"]    = df["sale_price"].notna() & (df["sale_price"] > 0)
    df["store"]         = store
    df["location"]      = location_from_filename(filename or str(filepath_or_bytes))
    return df

# ─── Parse XML ────────────────────────────────────────────────────────────────
def parse_xml(filepath, store, filename=""):
    tree = ET.parse(filepath)
    root = tree.getroot()
    items = (root.findall(".//artikal") or root.findall(".//Artikal")
             or root.findall(".//item") or root.findall(".//product"))
    if not items:
        raise ValueError(f"No product elements found. Root: {root.tag}")
    rows = []
    for item in items:
        def get(*tags):
            for tag in tags:
                el = item.find(tag)
                if el is not None and el.text:
                    return el.text.strip()
            return None
        rows.append({
            "name":          get("naziv", "Naziv", "name"),
            "brand":         get("marka", "Marka", "brand"),
            "barcode":       get("barkod", "Barkod", "barcode", "ean", "EAN"),
            "regular_price": get("mpc", "MPC", "cijena", "price"),
            "sale_price":    get("akcijska_cijena", "akcijskaCijena", "sale_price"),
            "category":      get("kategorija", "Kategorija", "category"),
            "quantity":      get("kolicina", "Kolicina", "neto_kolicina"),
            "unit":          get("jedinica", "Jedinica", "unit"),
        })
    df = pd.DataFrame(rows)
    for col in ["regular_price", "sale_price"]:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", ".").str.replace(r"[^\d.]", "", regex=True),
            errors="coerce"
        )
    df["barcode"]       = df["barcode"].astype(str).str.strip()
    df                  = df[df["barcode"].notna() & (df["barcode"] != "nan")]
    df["current_price"] = df["sale_price"].combine_first(df["regular_price"])
    df["is_on_sale"]    = df["sale_price"].notna() & (df["sale_price"] > 0)
    df["store"]         = store
    df["location"]      = location_from_filename(filename or filepath)
    return df

# ─── Process ZIP file ─────────────────────────────────────────────────────────
def process_zip(zip_bytes, store, file_ext="csv"):
    """Extract and process all files from a ZIP."""
    results = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist()
                 if n.lower().endswith(f".{file_ext}") and not n.startswith("__")]
        log(f"  ZIP contains {len(names)} .{file_ext} files")
        job["total"] = len(names)

        for i, name in enumerate(names):
            job["current_file"] = os.path.basename(name)
            log(f"\n  📂 [{i+1}/{len(names)}] {os.path.basename(name)}")
            try:
                data = zf.read(name)
                if file_ext == "csv":
                    df = parse_csv(io.BytesIO(data), store, filename=name)
                else:
                    tmp = f"/tmp/zipentry_{i}.xml"
                    with open(tmp, "wb") as f:
                        f.write(data)
                    df = parse_xml(tmp, store, filename=name)
                    os.remove(tmp)

                log(f"    {len(df)} products, {int(df['is_on_sale'].sum())} on sale")
                push_to_supabase(df, store)
                job["processed"] = i + 1
                results.append(len(df))
            except Exception as e:
                log(f"    ❌ Skipped: {e}")
                job["errors"].append(f"{name}: {e}")

    return results

# ─── Push to Supabase ─────────────────────────────────────────────────────────
def push_to_supabase(df, store):
    today = str(date.today())
    master, prices = [], []

    for _, row in df.iterrows():
        barcode = str(row.get("barcode", "")).strip()
        if not barcode or barcode == "nan":
            continue
        master.append({
            "barcode":  barcode,
            "name":     str(row.get("name", ""))[:300].strip(),
            "brand":    str(row.get("brand", ""))[:200].strip()   if pd.notna(row.get("brand"))    else None,
            "category": str(row.get("category", ""))[:200].strip() if pd.notna(row.get("category")) else None,
            "unit":     str(row.get("unit", ""))[:50].strip()     if pd.notna(row.get("unit"))     else None,
        })
        prices.append({
            "barcode":       barcode,
            "store":         store,
            "location":      str(row.get("location", ""))[:200],
            "price_date":    today,
            "current_price": float(row["current_price"]) if pd.notna(row.get("current_price")) else None,
            "regular_price": float(row["regular_price"]) if pd.notna(row.get("regular_price")) else None,
            "sale_price":    float(row["sale_price"])    if pd.notna(row.get("sale_price"))    else None,
            "is_on_sale":    bool(row.get("is_on_sale", False)),
        })

    n1 = upsert("master_products", master, conflict="barcode")
    log(f"  ✓ master_products: {n1} rows")
    n2 = upsert("store_prices", prices, conflict="barcode,store,location,price_date")
    log(f"  ✓ store_prices: {n2} rows")
    return n1

# ─── Auto-download a store ────────────────────────────────────────────────────
def auto_download(store):
    if store not in STORE_SOURCES:
        raise ValueError(f"No auto-download configured for '{store}'")

    url, file_type = STORE_SOURCES[store]
    log(f"  Downloading from {url}")

    headers = {"User-Agent": "Mozilla/5.0 (compatible; katalog-prices/1.0)"}
    resp = requests.get(url, headers=headers, timeout=60)
    if resp.status_code != 200:
        raise ValueError(f"Download failed: HTTP {resp.status_code}")

    log(f"  Downloaded {len(resp.content) / 1024:.0f} KB")

    if file_type == "zip_csv":
        process_zip(resp.content, store, "csv")
    elif file_type == "zip_xml":
        process_zip(resp.content, store, "xml")
    elif file_type == "index":
        raise ValueError(f"Index-based download for {store} not yet implemented — use manual upload")

# ─── Background job ───────────────────────────────────────────────────────────
def run_ingest(store, filepaths=None, auto=False):
    job.update({"running": True, "status": "processing", "processed": 0,
                "total": len(filepaths) if filepaths else 1,
                "errors": [], "log": []})

    try:
        if auto:
            log(f"🌐 Auto-download: {store}")
            auto_download(store)
        else:
            log(f"🚀 Ingesting {len(filepaths)} file(s) for '{store}'")
            for i, filepath in enumerate(filepaths):
                filename = os.path.basename(filepath)
                job["current_file"] = filename
                log(f"\n📂 [{i+1}/{len(filepaths)}] {filename}")
                try:
                    if filepath.lower().endswith(".zip"):
                        with open(filepath, "rb") as f:
                            ext = "xml" if store == "studenac" else "csv"
                            process_zip(f.read(), store, ext)
                    elif filepath.lower().endswith(".xml"):
                        df = parse_xml(filepath, store, filename)
                        log(f"  {len(df)} products")
                        push_to_supabase(df, store)
                    else:
                        df = parse_csv(filepath, store, filename)
                        log(f"  {len(df)} products, {int(df['is_on_sale'].sum())} on sale")
                        push_to_supabase(df, store)
                    job["processed"] = i + 1
                except Exception as e:
                    log(f"  ❌ Skipped: {e}")
                    job["errors"].append(f"{filename}: {e}")
                finally:
                    if os.path.exists(filepath):
                        os.remove(filepath)

        job["status"] = "done"
        log(f"\n✅ Done! Errors: {len(job['errors'])}")

    except Exception as e:
        job["status"] = "error"
        job["errors"].append(str(e))
        log(f"❌ Fatal: {e}")
    finally:
        job["running"] = False

# ─── HTML UI ──────────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html>
<head>
  <title>katalog-prices</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    *{box-sizing:border-box}
    body{font-family:system-ui,sans-serif;max-width:720px;margin:40px auto;padding:0 20px;background:#f5f5f5;color:#111}
    h1{font-size:22px;margin-bottom:2px}.sub{color:#666;font-size:14px;margin-top:0}
    .card{background:white;border:1px solid #e5e5e5;border-radius:10px;padding:20px;margin-bottom:16px}
    h2{font-size:15px;font-weight:600;margin:0 0 14px}
    label{font-size:13px;color:#555;display:block;margin-bottom:4px;font-weight:500}
    input[type=password],select{width:100%;padding:8px 10px;border:1px solid #ddd;border-radius:6px;font-size:14px;margin-bottom:14px}
    .drop{border:2px dashed #ddd;border-radius:8px;padding:24px;text-align:center;cursor:pointer;margin-bottom:14px;transition:border-color .2s}
    .drop:hover,.drop.drag{border-color:#111}.drop p{margin:0;font-size:14px;color:#888}
    .drop .cnt{color:#111;font-weight:500;margin-top:6px;font-size:13px}
    input[type=file]{display:none}
    .btn{background:#111;color:white;border:none;border-radius:6px;padding:10px 20px;font-size:14px;cursor:pointer;width:100%;margin-bottom:8px}
    .btn:disabled{background:#aaa;cursor:not-allowed}
    .btn.auto{background:#1a5c2a}
    .store-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:8px;margin-bottom:14px}
    .store-btn{padding:8px;border:1px solid #ddd;border-radius:6px;font-size:13px;cursor:pointer;background:white;text-align:center;transition:all .15s}
    .store-btn:hover{border-color:#111;background:#f9f9f9}
    .store-btn.sel{border-color:#111;background:#111;color:white}
    #log{background:#111;color:#00ff88;font-family:monospace;font-size:12px;padding:14px;border-radius:8px;min-height:140px;max-height:450px;overflow-y:auto;white-space:pre-wrap}
    .stats{display:flex;gap:20px;margin-bottom:10px;font-size:13px}.stats b{font-weight:600}
    .done{color:#16a34a}.err{color:#dc2626}.proc{color:#d97706}
    .hint{font-size:11px;color:#999;margin-top:-10px;margin-bottom:14px}
  </style>
</head>
<body>
  <h1>katalog-prices</h1>
  <p class="sub">Croatian grocery price pipeline</p>

  <div class="card">
    <label>Password</label>
    <input type="password" id="pw" placeholder="Enter password">

    <label>Store</label>
    <div class="store-grid">
      <div class="store-btn sel" onclick="selStore('konzum',this)">Konzum</div>
      <div class="store-btn" onclick="selStore('spar',this)">Spar</div>
      <div class="store-btn" onclick="selStore('lidl',this)">Lidl</div>
      <div class="store-btn" onclick="selStore('kaufland',this)">Kaufland</div>
      <div class="store-btn" onclick="selStore('plodine',this)">Plodine</div>
      <div class="store-btn" onclick="selStore('tommy',this)">Tommy</div>
      <div class="store-btn" onclick="selStore('studenac',this)">Studenac</div>
      <div class="store-btn" onclick="selStore('zabac',this)">Žabac</div>
      <div class="store-btn" onclick="selStore('ntl',this)">NTL</div>
    </div>
    <input type="hidden" id="store" value="konzum">
  </div>

  <div class="card">
    <h2>Upload files manually</h2>
    <label>CSV, XML or ZIP files — select multiple at once</label>
    <div class="drop" id="drop"
         onclick="document.getElementById('fi').click()"
         ondragover="event.preventDefault();this.classList.add('drag')"
         ondragleave="this.classList.remove('drag')"
         ondrop="handleDrop(event)">
      <p>Click to select or drag & drop here</p>
      <p class="cnt" id="cnt"></p>
    </div>
    <p class="hint">Tip: You can upload a ZIP file directly — it will unpack and process all CSVs inside automatically</p>
    <input type="file" id="fi" accept=".csv,.xml,.zip,.CSV,.XML,.ZIP" multiple onchange="updateCnt()">
    <button class="btn" id="btn-upload" onclick="go('upload')">Upload & ingest</button>
  </div>

  <div class="card">
    <h2>Auto-download from store website</h2>
    <p style="font-size:13px;color:#555;margin:0 0 14px">Directly downloads today's price files from the store's official URL</p>
    <button class="btn auto" id="btn-auto" onclick="go('auto')">Auto-download selected store</button>
    <p class="hint" id="auto-hint">Lidl and Tommy provide ZIP files — all locations processed automatically</p>
  </div>

  <div class="card">
    <div class="stats">
      <div>Status: <b><span id="st">idle</span></b></div>
      <div>Files: <b><span id="fc">—</span></b></div>
      <div>Current: <b><span id="cf" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">—</span></b></div>
    </div>
    <div id="log">Waiting...</div>
  </div>

<script>
let selectedStore = 'konzum';

function selStore(s, el) {
  selectedStore = s;
  document.getElementById('store').value = s;
  document.querySelectorAll('.store-btn').forEach(b => b.classList.remove('sel'));
  el.classList.add('sel');
  const autoSources = ['lidl','tommy'];
  document.getElementById('btn-auto').disabled = false;
  document.getElementById('auto-hint').textContent = 
    autoSources.includes(s) ? `${s.charAt(0).toUpperCase()+s.slice(1)} ZIP will be downloaded and all locations processed automatically` :
    'Auto-download not yet configured for this store — use manual upload';
}

function updateCnt(){
  const f=document.getElementById('fi').files;
  document.getElementById('cnt').textContent=f.length?`${f.length} file${f.length>1?'s':''} selected`:'';
}
function handleDrop(e){
  e.preventDefault();
  document.getElementById('drop').classList.remove('drag');
  document.getElementById('fi').files=e.dataTransfer.files;
  updateCnt();
}

async function go(mode) {
  const pw = document.getElementById('pw').value;
  const store = document.getElementById('store').value;
  if (!pw) { alert('Enter password'); return; }

  const fd = new FormData();
  fd.append('password', pw);
  fd.append('store', store);
  fd.append('mode', mode);

  if (mode === 'upload') {
    const files = document.getElementById('fi').files;
    if (!files.length) { alert('Select at least one file'); return; }
    for (const f of files) fd.append('files', f);
  }

  document.getElementById('btn-upload').disabled = true;
  document.getElementById('btn-auto').disabled = true;
  document.getElementById('log').textContent = mode === 'auto' ? 
    `Connecting to ${store} website...` : `Uploading files...`;

  const r = await fetch('/ingest', {method:'POST', body:fd});
  const d = await r.json();
  if (!r.ok) {
    document.getElementById('log').textContent = '❌ ' + d.error;
    document.getElementById('btn-upload').disabled = false;
    document.getElementById('btn-auto').disabled = false;
    return;
  }
  poll();
}

async function poll(){
  const d = await(await fetch('/status')).json();
  const cls = d.status==='done'?'done':d.status==='error'?'err':'proc';
  document.getElementById('st').className = cls;
  document.getElementById('st').textContent = d.status;
  document.getElementById('fc').textContent = `${d.processed}/${d.total}`;
  document.getElementById('cf').textContent = d.current_file || '—';
  document.getElementById('log').textContent = (d.log||[]).join('\\n');
  document.getElementById('log').scrollTop = 99999;
  if (d.status === 'processing') {
    setTimeout(poll, 1500);
  } else {
    document.getElementById('btn-upload').disabled = false;
    document.getElementById('btn-auto').disabled = false;
  }
}
</script>
</body>
</html>"""

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/ingest", methods=["POST"])
def start_ingest():
    if job["running"]:
        return jsonify({"error": "A job is already running. Wait for it to finish."}), 400
    if request.form.get("password") != UPLOAD_PASSWORD:
        return jsonify({"error": "Wrong password"}), 403

    store = request.form.get("store", "konzum")
    mode  = request.form.get("mode", "upload")

    if mode == "auto":
        threading.Thread(target=run_ingest, kwargs={"store": store, "auto": True}, daemon=True).start()
        return jsonify({"ok": True, "message": f"Auto-download started for {store}"})

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    filepaths = []
    for i, f in enumerate(files):
        name = f.filename.lower()
        ext = "zip" if name.endswith(".zip") else "xml" if name.endswith(".xml") else "csv"
        tmp = f"/tmp/prices_{store}_{i}_{date.today()}.{ext}"
        f.save(tmp)
        filepaths.append(tmp)

    threading.Thread(target=run_ingest, kwargs={"store": store, "filepaths": filepaths}, daemon=True).start()
    return jsonify({"ok": True, "message": f"Started {len(filepaths)} file(s) for {store}"})

@app.route("/status")
def status():
    return jsonify({k: job[k] for k in job})

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=False)
