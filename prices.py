"""
prices.py — katalog-prices web service
A simple Flask app that wraps ingest.py so it can run on Render.

- Password protected (same pattern as upload.py in katalog.ai)
- Trigger ingestion from a browser
- Runs in background thread, check status live
"""

import os
import threading
import requests
import pandas as pd
from datetime import date
from flask import Flask, jsonify, request, render_template_string
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

UPLOAD_PASSWORD   = os.environ.get("UPLOAD_PASSWORD", "katalog2026")
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "")

# ─── Global job state ─────────────────────────────────────────────────────────
job = {
    "running":   False,
    "store":     None,
    "status":    "idle",
    "processed": 0,
    "errors":    [],
    "log":       [],
}

def log(msg):
    print(msg)
    job["log"].append(msg)
    if len(job["log"]) > 200:
        job["log"] = job["log"][-200:]

# ─── Supabase helpers ─────────────────────────────────────────────────────────
def db_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
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

# ─── Parse Konzum CSV ─────────────────────────────────────────────────────────
def parse_konzum_csv(filepath):
    for encoding in ["cp1250", "utf-8", "utf-8-sig", "latin-1"]:
        try:
            df = pd.read_csv(filepath, sep=";", encoding=encoding, dtype=str, skipinitialspace=True)
            log(f"  Opened with encoding: {encoding}")
            break
        except Exception:
            continue
    else:
        raise ValueError("Could not open file with any known encoding")

    df.columns = [c.strip() for c in df.columns]

    rename = {
        "naziv":                                         "name",
        "marka":                                         "brand",
        "neto kolièina":                                 "quantity",
        "jedinica mjere":                                "unit",
        "MPC (EUR)":                                     "regular_price",
        "MPC za vrijeme posebnog oblika prodaje (EUR)":  "sale_price",
        "Najniža cijena u posljednjih 30 dana (EUR)":    "lowest_30d_price",
        "sidrena cijena na 2.5.2025. (EUR)":             "anchor_price",
        "barkod":                                        "barcode",
        "kategorija proizvoda":                          "category",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    for col in ["regular_price", "sale_price", "lowest_30d_price", "anchor_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.strip()
                    .str.replace(",", ".", regex=False)
                    .str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            )

    if "barcode" in df.columns:
        df["barcode"] = df["barcode"].astype(str).str.strip()
        df = df[df["barcode"].notna() & (df["barcode"] != "") & (df["barcode"] != "nan")]

    df["current_price"] = df["sale_price"].combine_first(df.get("regular_price"))
    df["is_on_sale"]    = df["sale_price"].notna()
    df["store"]         = "konzum"

    return df

# ─── Parse Studenac XML ───────────────────────────────────────────────────────
def parse_studenac_xml(filepath):
    import xml.etree.ElementTree as ET
    tree = ET.parse(filepath)
    root = tree.getroot()

    items = root.findall(".//artikal") or root.findall(".//item") or root.findall(".//product")
    if not items:
        raise ValueError(f"No product elements found. Root tag: {root.tag}, children: {[c.tag for c in list(root)[:5]]}")

    rows = []
    for item in items:
        def get(tag):
            el = item.find(tag)
            return el.text.strip() if el is not None and el.text else None
        rows.append({
            "name":          get("naziv") or get("name"),
            "brand":         get("marka") or get("brand"),
            "barcode":       get("barkod") or get("barcode") or get("ean"),
            "regular_price": get("mpc") or get("cijena") or get("price"),
            "sale_price":    get("akcijska_cijena") or get("sale_price"),
            "category":      get("kategorija") or get("category"),
            "quantity":      get("kolicina") or get("quantity"),
            "unit":          get("jedinica") or get("unit"),
        })

    df = pd.DataFrame(rows)
    for col in ["regular_price", "sale_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".").str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            )
    df["current_price"] = df["sale_price"].combine_first(df.get("regular_price"))
    df["is_on_sale"]    = df["sale_price"].notna()
    df["store"]         = "studenac"
    return df

# ─── Background ingest job ────────────────────────────────────────────────────
def run_ingest(store, filepath):
    job["running"]   = True
    job["store"]     = store
    job["status"]    = "processing"
    job["processed"] = 0
    job["errors"]    = []
    job["log"]       = []

    try:
        log(f"📂 Loading {store} file: {filepath}")

        if store == "konzum":
            df = parse_konzum_csv(filepath)
        elif store == "studenac":
            df = parse_studenac_xml(filepath)
        else:
            df = pd.read_csv(filepath, sep=None, engine="python", encoding="utf-8", dtype=str)
            df["store"] = store
            log(f"  ⚠️ Generic parser used for {store}")

        log(f"  ✓ Loaded {len(df)} products, {int(df['is_on_sale'].sum())} on sale")

        # Build master_products records
        master = []
        for _, row in df.dropna(subset=["barcode"]).iterrows():
            barcode = str(row.get("barcode", "")).strip()
            if not barcode or barcode == "nan":
                continue
            master.append({
                "barcode":  barcode,
                "name":     str(row.get("name", ""))[:300].strip(),
                "brand":    str(row.get("brand", ""))[:200].strip() if pd.notna(row.get("brand")) else None,
                "category": str(row.get("category", ""))[:200].strip() if pd.notna(row.get("category")) else None,
                "unit":     str(row.get("unit", ""))[:50].strip() if pd.notna(row.get("unit")) else None,
            })

        log(f"⬆️  Upserting {len(master)} rows into master_products...")
        n = upsert("master_products", master)
        log(f"  ✓ master_products: {n} rows saved")

        # Build store_prices records
        today = str(date.today())
        prices = []
        for _, row in df.dropna(subset=["barcode"]).iterrows():
            barcode = str(row.get("barcode", "")).strip()
            if not barcode or barcode == "nan":
                continue
            prices.append({
                "barcode":       barcode,
                "store":         store,
                "price_date":    today,
                "current_price": float(row["current_price"]) if pd.notna(row.get("current_price")) else None,
                "regular_price": float(row["regular_price"]) if pd.notna(row.get("regular_price")) else None,
                "sale_price":    float(row["sale_price"])    if pd.notna(row.get("sale_price"))    else None,
                "is_on_sale":    bool(row.get("is_on_sale", False)),
            })

        log(f"⬆️  Upserting {len(prices)} rows into store_prices...")
        n = upsert("store_prices", prices)
        log(f"  ✓ store_prices: {n} rows saved")

        job["processed"] = len(df)
        job["status"]    = "done"
        log("✅ Ingest complete!")

    except Exception as e:
        job["status"] = "error"
        job["errors"].append(str(e))
        log(f"❌ Error: {e}")
    finally:
        job["running"] = False
        # Clean up uploaded file
        if os.path.exists(filepath):
            os.remove(filepath)

# ─── HTML UI ──────────────────────────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>katalog-prices</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: system-ui, sans-serif; max-width: 640px; margin: 40px auto; padding: 0 20px; background: #f9f9f9; }
    h1 { font-size: 22px; margin-bottom: 4px; }
    p.sub { color: #666; font-size: 14px; margin-top: 0; }
    .card { background: white; border: 1px solid #e5e5e5; border-radius: 10px; padding: 20px; margin-bottom: 16px; }
    label { font-size: 13px; color: #555; display: block; margin-bottom: 4px; }
    input, select { width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px; box-sizing: border-box; margin-bottom: 12px; }
    button { background: #111; color: white; border: none; border-radius: 6px; padding: 10px 20px; font-size: 14px; cursor: pointer; }
    button:disabled { background: #aaa; cursor: not-allowed; }
    #log { background: #111; color: #00ff88; font-family: monospace; font-size: 12px; padding: 14px; border-radius: 8px; min-height: 100px; max-height: 300px; overflow-y: auto; white-space: pre-wrap; }
    .status-done { color: #16a34a; font-weight: 600; }
    .status-error { color: #dc2626; font-weight: 600; }
    .status-processing { color: #d97706; font-weight: 600; }
  </style>
</head>
<body>
  <h1>katalog-prices</h1>
  <p class="sub">Croatian grocery price data pipeline</p>

  <div class="card">
    <label>Password</label>
    <input type="password" id="password" placeholder="Enter password">

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

    <label>Price file (CSV or XML)</label>
    <input type="file" id="file" accept=".csv,.xml,.CSV,.XML">

    <button id="btn" onclick="startIngest()">Start ingest</button>
  </div>

  <div class="card">
    <div id="status" style="font-size:14px;margin-bottom:10px">Status: <span>idle</span></div>
    <div id="log">Waiting...</div>
  </div>

<script>
async function startIngest() {
  const pw    = document.getElementById('password').value;
  const store = document.getElementById('store').value;
  const file  = document.getElementById('file').files[0];
  if (!pw || !file) { alert('Enter password and select a file'); return; }

  const fd = new FormData();
  fd.append('password', pw);
  fd.append('store', store);
  fd.append('file', file);

  document.getElementById('btn').disabled = true;
  document.getElementById('log').textContent = 'Uploading file...';

  const resp = await fetch('/ingest', { method: 'POST', body: fd });
  const data = await resp.json();
  if (!resp.ok) {
    document.getElementById('log').textContent = '❌ ' + data.error;
    document.getElementById('btn').disabled = false;
    return;
  }
  pollStatus();
}

async function pollStatus() {
  const resp = await fetch('/status');
  const data = await resp.json();
  const statusEl = document.getElementById('status');
  const logEl    = document.getElementById('log');

  const cls = data.status === 'done' ? 'status-done' : data.status === 'error' ? 'status-error' : 'status-processing';
  statusEl.innerHTML = `Status: <span class="${cls}">${data.status}</span> — ${data.processed} products`;
  logEl.textContent = (data.log || []).join('\\n');
  logEl.scrollTop = logEl.scrollHeight;

  if (data.status === 'processing') {
    setTimeout(pollStatus, 1500);
  } else {
    document.getElementById('btn').disabled = false;
  }
}
</script>
</body>
</html>
"""

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/ingest", methods=["POST"])
def start_ingest():
    if job["running"]:
        return jsonify({"error": "A job is already running. Wait for it to finish."}), 400

    password = request.form.get("password", "")
    if password != UPLOAD_PASSWORD:
        return jsonify({"error": "Wrong password"}), 403

    store = request.form.get("store", "konzum")
    file  = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    # Save file temporarily
    tmp_path = f"/tmp/prices_{store}_{date.today()}.{'xml' if store == 'studenac' else 'csv'}"
    file.save(tmp_path)

    thread = threading.Thread(target=run_ingest, args=(store, tmp_path), daemon=True)
    thread.start()

    return jsonify({"ok": True, "message": f"Ingest started for {store}"})

@app.route("/status")
def status():
    return jsonify({
        "running":   job["running"],
        "store":     job["store"],
        "status":    job["status"],
        "processed": job["processed"],
        "errors":    job["errors"],
        "log":       job["log"],
    })

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
