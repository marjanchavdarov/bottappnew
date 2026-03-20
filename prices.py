"""
prices.py — katalog-prices web service
Flask app that ingests Croatian store price files into Supabase.
Supports uploading multiple files at once.
"""

import os
import threading
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

# ─── Global job state ─────────────────────────────────────────────────────────
job = {
    "running":      False,
    "status":       "idle",
    "processed":    0,
    "total":        0,
    "current_file": "",
    "errors":       [],
    "log":          [],
}

def log(msg):
    print(msg)
    job["log"].append(msg)
    if len(job["log"]) > 500:
        job["log"] = job["log"][-500:]

# ─── Supabase ─────────────────────────────────────────────────────────────────
def db_headers():
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",
    }

def upsert(table, records, batch_size=500):
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=db_headers(),
            json=batch,
            timeout=30,
        )
        if resp.status_code not in (200, 201):
            log(f"  ❌ {table} error: {resp.status_code} {resp.text[:200]}")
        else:
            total += len(batch)
    return total

# ─── Fuzzy column finder ──────────────────────────────────────────────────────
# Matches column names even when encoding garbles special Croatian characters.
# Each entry: (standard_name, [keywords_to_search_for_in_column_name])
COLUMN_HINTS = [
    ("name",             ["naziv"]),
    ("brand",            ["marka", "brand"]),
    ("quantity",         ["neto", "kolici", "koli"]),
    ("unit",             ["jedinica mjere", "jedinica"]),
    ("regular_price",    ["maloprodajna", "mpc (eur)", "mpc(eur)", "mpc eur"]),
    ("sale_price",       ["posebnog oblika", "posebno", "akcij", "sale"]),
    ("lowest_30d_price", ["30 dan", "30dan", "najni"]),
    ("anchor_price",     ["sidrena", "anchor"]),
    ("barcode",          ["barkod", "barcode", "ean"]),
    ("category",         ["kategorij", "category"]),
]

def fuzzy_rename(df):
    """Rename columns using keyword matching — works across encodings."""
    cols_lower = {c: c.lower() for c in df.columns}
    rename_map = {}
    already_mapped = set()

    for std_name, hints in COLUMN_HINTS:
        for col, col_l in cols_lower.items():
            if col in rename_map:
                continue
            if std_name in already_mapped:
                continue
            if any(h in col_l for h in hints):
                rename_map[col] = std_name
                already_mapped.add(std_name)
                break

    df = df.rename(columns=rename_map)

    found   = sorted(already_mapped)
    missing = [h[0] for h in COLUMN_HINTS if h[0] not in already_mapped]
    log(f"  Columns mapped: {found}")
    if missing:
        log(f"  ⚠️  Not found (will be null): {missing}")

    return df

# ─── Parse CSV (Konzum, Spar, Lidl, etc.) ────────────────────────────────────
def parse_csv(filepath, store):
    df = None
    for encoding in ["cp1250", "utf-8", "utf-8-sig", "latin-1"]:
        ("regular_price",    ["maloprodajna", "mpc (eur)", "mpc(eur)", "mpc eur"]),
            log(f"  Opened with encoding: {encoding} — {len(df)} rows")
            break
        except Exception:
            continue

    if df is None:
        raise ValueError("Could not open CSV with any known encoding")

    df.columns = [c.strip() for c in df.columns]
    df = fuzzy_rename(df)

    # Ensure all price columns exist
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
        raise ValueError("No barcode column found — cannot process this file")

    df["barcode"]       = df["barcode"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
    df                  = df[df["barcode"].notna() & (df["barcode"] != "") & (df["barcode"] != "nan")]
    df["current_price"] = df["sale_price"].combine_first(df["regular_price"])
    df["is_on_sale"]    = df["sale_price"].notna() & (df["sale_price"] > 0)
    df["store"]         = store

    return df

# ─── Parse Studenac XML ───────────────────────────────────────────────────────
def parse_xml(filepath, store):
    tree  = ET.parse(filepath)
    root  = tree.getroot()
    items = (root.findall(".//artikal") or root.findall(".//Artikal")
             or root.findall(".//item")  or root.findall(".//product"))

    if not items:
        raise ValueError(f"No product elements found. Root: {root.tag}, children: {[c.tag for c in list(root)[:5]]}")

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
    return df

# ─── Push dataframe to Supabase ───────────────────────────────────────────────
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
            "brand":    str(row.get("brand", ""))[:200].strip() if pd.notna(row.get("brand")) else None,
            "category": str(row.get("category", ""))[:200].strip() if pd.notna(row.get("category")) else None,
            "unit":     str(row.get("unit", ""))[:50].strip()   if pd.notna(row.get("unit"))     else None,
        })

        prices.append({
            "barcode":       barcode,
            "store":         store,
            "price_date":    today,
            "current_price": float(row["current_price"]) if pd.notna(row.get("current_price")) else None,
            "regular_price": float(row["regular_price"]) if pd.notna(row.get("regular_price")) else None,
            "sale_price":    float(row["sale_price"])    if pd.notna(row.get("sale_price"))    else None,
            "is_on_sale":    bool(row.get("is_on_sale", False)),
        })

    n1 = upsert("master_products", master)
    log(f"  ✓ master_products: {n1} rows saved")

    n2 = upsert("store_prices", prices)
    log(f"  ✓ store_prices: {n2} rows saved")

    return n1

# ─── Background job ───────────────────────────────────────────────────────────
def run_ingest(store, filepaths):
    job.update({"running": True, "status": "processing", "processed": 0,
                "total": len(filepaths), "errors": [], "log": []})

    log(f"🚀 Starting ingest: {len(filepaths)} file(s) for '{store}'")

    for i, filepath in enumerate(filepaths):
        filename = os.path.basename(filepath)
        job["current_file"] = filename
        log(f"\n📂 [{i+1}/{len(filepaths)}] {filename}")

        try:
            df = parse_xml(filepath, store) if filepath.lower().endswith(".xml") else parse_csv(filepath, store)
            log(f"  Parsed: {len(df)} products, {int(df['is_on_sale'].sum())} on sale")
            push_to_supabase(df, store)
            job["processed"] = i + 1
        except Exception as e:
            log(f"  ❌ Skipped: {e}")
            job["errors"].append(f"{filename}: {e}")
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    job["status"]  = "done"
    job["running"] = False
    log(f"\n✅ Finished! {job['processed']}/{len(filepaths)} files processed. Errors: {len(job['errors'])}")

# ─── HTML UI ──────────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html>
<head>
  <title>katalog-prices</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    *{box-sizing:border-box}
    body{font-family:system-ui,sans-serif;max-width:680px;margin:40px auto;padding:0 20px;background:#f5f5f5;color:#111}
    h1{font-size:22px;margin-bottom:2px}
    .sub{color:#666;font-size:14px;margin-top:0}
    .card{background:white;border:1px solid #e5e5e5;border-radius:10px;padding:20px;margin-bottom:16px}
    label{font-size:13px;color:#555;display:block;margin-bottom:4px;font-weight:500}
    input[type=password],select{width:100%;padding:8px 10px;border:1px solid #ddd;border-radius:6px;font-size:14px;margin-bottom:14px}
    .drop{border:2px dashed #ddd;border-radius:8px;padding:28px;text-align:center;cursor:pointer;margin-bottom:14px;transition:border-color .2s}
    .drop:hover,.drop.drag{border-color:#111}
    .drop p{margin:0;font-size:14px;color:#888}
    .drop .cnt{color:#111;font-weight:500;margin-top:6px;font-size:13px}
    input[type=file]{display:none}
    button{background:#111;color:white;border:none;border-radius:6px;padding:10px 20px;font-size:14px;cursor:pointer;width:100%}
    button:disabled{background:#aaa;cursor:not-allowed}
    #log{background:#111;color:#00ff88;font-family:monospace;font-size:12px;padding:14px;border-radius:8px;min-height:140px;max-height:400px;overflow-y:auto;white-space:pre-wrap}
    .stats{display:flex;gap:20px;margin-bottom:10px;font-size:13px}
    .stats b{font-weight:600}
    .done{color:#16a34a}.err{color:#dc2626}.proc{color:#d97706}
  </style>
</head>
<body>
  <h1>katalog-prices</h1>
  <p class="sub">Croatian grocery price pipeline</p>

  <div class="card">
    <label>Password</label>
    <input type="password" id="pw" placeholder="Enter password">
    <label>Store</label>
    <select id="store">
      <option value="konzum">Konzum (CSV)</option>
      <option value="studenac">Studenac (XML)</option>
      <option value="spar">Spar (CSV)</option>
      <option value="lidl">Lidl (CSV)</option>
      <option value="kaufland">Kaufland (CSV)</option>
      <option value="plodine">Plodine (CSV)</option>
      <option value="tommy">Tommy (CSV)</option>
    </select>
    <label>Files — you can select multiple at once</label>
    <div class="drop" id="drop"
         onclick="document.getElementById('fi').click()"
         ondragover="event.preventDefault();this.classList.add('drag')"
         ondragleave="this.classList.remove('drag')"
         ondrop="handleDrop(event)">
      <p>Click to select files or drag & drop</p>
      <p class="cnt" id="cnt"></p>
    </div>
    <input type="file" id="fi" accept=".csv,.xml,.CSV,.XML" multiple onchange="updateCnt()">
    <button id="btn" onclick="go()">Start ingest</button>
  </div>

  <div class="card">
    <div class="stats">
      <div>Status: <b><span id="st" class="">idle</span></b></div>
      <div>Files: <b><span id="fc">—</span></b></div>
      <div>Current: <b><span id="cf">—</span></b></div>
    </div>
    <div id="log">Waiting...</div>
  </div>

<script>
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
async function go(){
  const pw=document.getElementById('pw').value;
  const store=document.getElementById('store').value;
  const files=document.getElementById('fi').files;
  if(!pw||!files.length){alert('Enter password and select files');return;}
  const fd=new FormData();
  fd.append('password',pw);fd.append('store',store);
  for(const f of files)fd.append('files',f);
  document.getElementById('btn').disabled=true;
  document.getElementById('log').textContent=`Uploading ${files.length} file(s)...`;
  const r=await fetch('/ingest',{method:'POST',body:fd});
  const d=await r.json();
  if(!r.ok){document.getElementById('log').textContent='❌ '+d.error;document.getElementById('btn').disabled=false;return;}
  poll();
}
async function poll(){
  const d=await(await fetch('/status')).json();
  const cls=d.status==='done'?'done':d.status==='error'?'err':'proc';
  document.getElementById('st').className=cls;
  document.getElementById('st').textContent=d.status;
  document.getElementById('fc').textContent=`${d.processed}/${d.total}`;
  document.getElementById('cf').textContent=d.current_file||'—';
  document.getElementById('log').textContent=(d.log||[]).join('\\n');
  document.getElementById('log').scrollTop=99999;
  if(d.status==='processing')setTimeout(poll,1500);
  else document.getElementById('btn').disabled=false;
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
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    filepaths = []
    for i, f in enumerate(files):
        ext = "xml" if f.filename.lower().endswith(".xml") else "csv"
        tmp = f"/tmp/prices_{store}_{i}_{date.today()}.{ext}"
        f.save(tmp)
        filepaths.append(tmp)

    threading.Thread(target=run_ingest, args=(store, filepaths), daemon=True).start()
    return jsonify({"ok": True, "message": f"Started {len(filepaths)} file(s) for {store}"})

@app.route("/status")
def status():
    return jsonify({k: job[k] for k in job})

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=False)
