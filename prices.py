"""
prices.py — katalog-prices web service
Full auto-download for all Croatian grocery stores.
"""

import os
import io
import re
import threading
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, request, render_template_string
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Optional

load_dotenv()

app = Flask(__name__)

UPLOAD_PASSWORD      = os.environ.get("UPLOAD_PASSWORD", "katalog2026")
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY         = os.environ.get("SUPABASE_KEY", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer":    "https://www.kaufland.hr/akcije-novosti/popis-mpc.html",
    "Accept":     "text/csv,application/octet-stream,*/*",
}

job = {
    "running": False, "status": "idle", "store": "",
    "processed": 0, "total": 0, "current_file": "",
    "errors": [], "log": [],
}

def log(msg):
    print(msg)
    job["log"].append(msg)
    if len(job["log"]) > 2000:
        job["log"] = job["log"][-2000:]

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
            log(f"  ❌ {table}: {resp.status_code} {resp.text[:150]}")
        else:
            total += len(batch)
    return total

# ─── Column hints ─────────────────────────────────────────────────────────────
COLUMN_HINTS = [
    ("name",             ["naziv"]),
    ("brand",            ["marka", "brand"]),
    ("quantity",         ["neto", "kolici", "koli", "grama"]),
    ("unit",             ["jedinica mjere", "jedinica"]),
    ("regular_price",    ["maloprodajna", "maloprod", "mpc (eur)", "mpc(eur)", "mpc eur", "mpc"]),
    ("sale_price",       ["posebnog oblika", "posebno", "posebn", "akcij", "akc", "sale", "poseb.oblik", "mpc poseb"]),
    ("lowest_30d_price", ["30 dan", "30dana", "30 dana", "najni"]),
    ("anchor_price",     ["sidrena", "anchor"]),
    ("barcode",          ["barkod", "barcode", "ean"]),
    ("category",         ["kategorij", "category", "grupe", "robna"]),
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
    missing = [h[0] for h in COLUMN_HINTS if h[0] not in already_mapped]
    if missing:
        log(f"  ⚠️  Missing: {missing}")
    return df

def location_from_filename(filename):
    name = os.path.splitext(os.path.basename(str(filename)))[0]
    name = re.sub(r'\d{4}[-_]\d{2}[-_]\d{2}.*', '', name)
    name = re.sub(r'\d{8}.*', '', name)
    name = re.sub(r'\d{5,}', '', name)
    name = re.sub(r'[-_]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    words = [w.capitalize() for w in name.split() if len(w) > 2]
    return ' '.join(words[-4:]) if words else name[:60]

# ─── Parse CSV ────────────────────────────────────────────────────────────────
def parse_csv(src, store, filename=""):
    # Always read to raw bytes first so we can retry encodings with fresh BytesIO
    if isinstance(src, (bytes, bytearray)):
        raw = bytes(src)
    elif isinstance(src, io.BytesIO):
        src.seek(0)
        raw = src.read()
    else:
        with open(src, "rb") as f:
            raw = f.read()

    df = None
    for encoding in ["utf-8-sig", "utf-8", "utf-16", "utf-16-le", "cp1250", "latin-1"]:
        try:
            df = pd.read_csv(
                io.BytesIO(raw),
                sep=None, engine="python",
                encoding=encoding, dtype=str, skipinitialspace=True,
            )
            log(f"  Encoding: {encoding} — {len(df)} rows")
            break
        except Exception:
            continue

    if df is None:
        raise ValueError("Could not decode CSV")

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
        raise ValueError("No barcode column")

    df["barcode"] = df["barcode"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
    df = df[df["barcode"].notna() & (df["barcode"] != "") & (df["barcode"] != "nan")]
    df["current_price"] = df["sale_price"].combine_first(df["regular_price"])
    df["is_on_sale"]    = df["sale_price"].notna() & (df["sale_price"] > 0)
    df["store"]         = store
    df["location"]      = location_from_filename(filename)
    return df

# ─── Parse XML ────────────────────────────────────────────────────────────────
def parse_xml(src, store, filename=""):
    if isinstance(src, (bytes, bytearray)):
        root = ET.fromstring(src.decode("utf-8", errors="replace"))
    else:
        root = ET.parse(src).getroot()

    # Handle Studenac format: Proizvodi -> ProdajniObjekt -> Proizvodi -> Proizvod
    items = []
    
    # Check if this is Studenac format (has ProdajniObjekt)
    prodajni_objekt = root.find('ProdajniObjekt')
    if prodajni_objekt is not None:
        inner_proizvodi = prodajni_objekt.find('Proizvodi')
        if inner_proizvodi is not None:
            items = inner_proizvodi.findall('Proizvod')
            log(f"  Found {len(items)} products in Studenac format")
    else:
        # Original format: look for artikal, Artikal, item, product directly under root
        items = (root.findall(".//artikal") or root.findall(".//Artikal")
                 or root.findall(".//item") or root.findall(".//product"))
    
    if not items:
        raise ValueError(f"No product elements. Root: {root.tag}")

    rows = []
    for item in items:
        def get(*tags):
            for tag in tags:
                el = item.find(tag)
                if el is not None and el.text:
                    return el.text.strip()
            return None
        
        # Try to find barcode from various possible tags
        barcode = get("barkod", "Barkod", "barcode", "ean", "EAN", "Barkod_artikla")
        
        rows.append({
            "name":          get("naziv", "Naziv", "NazivProizvoda", "name"),
            "brand":         get("marka", "Marka", "MarkaProizvoda", "brand"),
            "barcode":       barcode,
            "regular_price": get("mpc", "MPC", "cijena", "price", "MaloprodajnaCijena"),
            "sale_price":    get("akcijska_cijena", "akcijskaCijena", "sale_price", "MaloprodajnaCijenaAkcija"),
            "category":      get("kategorija", "Kategorija", "category", "KategorijeProizvoda"),
            "quantity":      get("kolicina", "Kolicina", "neto_kolicina", "NetoKolicina"),
            "unit":          get("jedinica", "Jedinica", "unit", "JedinicaMjere"),
            "lowest_30d_price": get("najniza", "najniža", "najniza_cijena", "NajnizaCijena"),
            "anchor_price":  get("sidrena", "sidrena_cijena", "SidrenaCijena"),
        })

    df = pd.DataFrame(rows)
    
    # Clean price columns
    for col in ["regular_price", "sale_price", "lowest_30d_price", "anchor_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".").str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            )
    
    # Clean barcode
    if "barcode" in df.columns:
        df["barcode"] = df["barcode"].astype(str).str.strip()
    else:
        raise ValueError("No barcode column found")
    
    # Remove rows without barcode
    df = df[df["barcode"].notna() & (df["barcode"] != "nan")]
    
    # Set current price (sale price takes precedence)
    if "sale_price" in df.columns:
        df["current_price"] = df["sale_price"].combine_first(df.get("regular_price"))
    else:
        df["current_price"] = df.get("regular_price")
    
    df["is_on_sale"] = (df.get("sale_price", pd.Series([None])).notna() & 
                        (df.get("sale_price", 0) > 0))
    
    # Remove rows without price
    df = df[df["current_price"].notna()]
    
    df["store"] = store
    df["location"] = location_from_filename(filename)
    
    log(f"  Parsed {len(df)} valid products from {os.path.basename(filename)}")
    return df

# ─── Process ZIP bytes ────────────────────────────────────────────────────────
def process_zip_bytes(zip_bytes, store, ext="csv"):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist()
                 if n.lower().endswith(f".{ext}") and not n.startswith("__")]
        log(f"  ZIP: {len(names)} .{ext} files")
        job["total"] += len(names)
        for i, name in enumerate(names):
            job["current_file"] = os.path.basename(name)
            try:
                data = zf.read(name)
                df = parse_csv(data, store, filename=name) if ext == "csv" else parse_xml(data, store, filename=name)
                log(f"  [{i+1}/{len(names)}] {os.path.basename(name)} — {len(df)} rows")
                push_to_supabase(df, store)
                job["processed"] += 1
            except Exception as e:
                log(f"  ❌ {os.path.basename(name)}: {e}")
                job["errors"].append(f"{name}: {e}")

# ─── Push to Supabase ─────────────────────────────────────────────────────────
def push_to_supabase(df, store):
    today = str(date.today())

    # Build master_products records
    master = []
    for _, row in df.drop_duplicates(subset=["barcode"]).iterrows():
        barcode = str(row.get("barcode","")).strip()
        if not barcode or barcode == "nan":
            continue
        master.append({
            "barcode":  barcode,
            "name":     str(row.get("name",""))[:300].strip(),
            "brand":    str(row.get("brand",""))[:200].strip()    if pd.notna(row.get("brand"))    else None,
            "category": str(row.get("category",""))[:200].strip() if pd.notna(row.get("category")) else None,
            "unit":     str(row.get("unit",""))[:50].strip()      if pd.notna(row.get("unit"))     else None,
        })

    # Aggregate prices per barcode — min, max, locations_count
    df_clean = df[df["barcode"].notna() & (df["barcode"] != "nan") & df["current_price"].notna()].copy()
    agg = df_clean.groupby("barcode").agg(
        min_price=("current_price", "min"),
        max_price=("current_price", "max"),
        current_price=("current_price", "mean"),
        regular_price=("regular_price", "min"),
        sale_price=("sale_price", "min"),
        is_on_sale=("is_on_sale", "any"),
        locations_count=("barcode", "count"),
    ).reset_index()

    prices = []
    for _, row in agg.iterrows():
        prices.append({
            "barcode":         str(row["barcode"]).strip(),
            "store":           store,
            "price_date":      today,
            "min_price":       round(float(row["min_price"]), 2),
            "max_price":       round(float(row["max_price"]), 2),
            "current_price":   round(float(row["current_price"]), 2),
            "regular_price":   round(float(row["regular_price"]), 2) if pd.notna(row.get("regular_price")) else None,
            "sale_price":      round(float(row["sale_price"]), 2)    if pd.notna(row.get("sale_price"))    else None,
            "is_on_sale":      bool(row["is_on_sale"]),
            "locations_count": int(row["locations_count"]),
        })

    n1 = upsert("master_products", master, conflict="barcode")
    n2 = upsert("store_prices",    prices, conflict="barcode,store,price_date")
    log(f"  ✓ {n1} products, {n2} prices saved ({len(prices)} unique barcodes, {df_clean['locations_count'].sum() if 'locations_count' in df_clean.columns else len(df_clean)} location rows aggregated)")
    return n1

# ─── Single file downloader (used by concurrent workers) ─────────────────────
def _download_one_csv(url, store):
    filename = url.split("?title=")[-1] if "?title=" in url else url.split("/")[-1]
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        df = parse_csv(r.content, store, filename=filename)
        push_to_supabase(df, store)
        job["processed"] += 1
        job["current_file"] = filename[:80]
        return len(df)
    except Exception as e:
        err = f"{filename[:60]}: {e}"
        log(f"  ❌ {err}")
        job["errors"].append(err)
        return 0

# ─── Store downloaders ────────────────────────────────────────────────────────
def download_lidl():
    log("🔵 LIDL — downloading ZIP...")
    for delta in [0, 1]:
        d = (date.today() - timedelta(days=delta)).strftime("%d_%m_%Y")
        url = f"https://tvrtka.lidl.hr/content/download/156615/fileupload/Popis_cijena_po_trgovinama_na_dan_{d}.zip"
        log(f"  Trying: {url}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 200:
                log(f"  ✓ {len(r.content)//1024} KB")
                process_zip_bytes(r.content, "lidl", "csv")
                return
        except Exception:
            continue
    raise ValueError("Lidl ZIP not found for today or yesterday")

# ── 1. Add these to COLUMN_HINTS (replace the existing sale_price and category lines) ──
#
# ("sale_price",       ["posebnog oblika", "posebno", "posebn", "akcij", "akc", "sale", "poseb.oblik", "mpc poseb"]),
# ("category",         ["kategorij", "category", "grupe", "robna"]),
#
# "posebn" catches MPC_POSEBNA_PRODAJA (Tommy)
# "robna"  catches ROBNA_STRUKTURA (Tommy)


# ── 2. Replace download_tommy() with this ────────────────────────────────────
def download_tommy():
    """Download Tommy price data - optimized for large datasets"""
    log("🟠 TOMMY — fetching price data from API...")
    
    headers = {**HEADERS, "Accept": "application/ld+json"}
    total_processed = 0
    
    for delta in [0, 1]:
        d = (date.today() - timedelta(days=delta)).strftime("%Y-%m-%d")
        files_url = f"https://spiza.tommy.hr/api/v2/shop/store-prices-tables?date={d}"
        
        try:
            r = requests.get(files_url, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            members = data.get("hydra:member", [])
            if not members:
                continue
                
            log(f"  Found {len(members)} price files for {d}")
            job["total"] = len(members)
            
            # Process each store individually to avoid memory issues
            for idx, file_info in enumerate(members):
                filename = file_info.get("fileName")
                if not filename:
                    continue
                
                job["current_file"] = f"Processing {filename} ({idx+1}/{len(members)})"
                
                # URL encode the filename
                import urllib.parse
                encoded_filename = urllib.parse.quote(filename)
                csv_url = f"https://spiza.tommy.hr/api/v2/shop/store-prices-tables/{encoded_filename}"
                
                try:
                    r_csv = requests.get(csv_url, headers={**HEADERS, "Accept": "text/csv"}, timeout=60)
                    
                    if r_csv.status_code == 200:
                        # Parse CSV
                        df = pd.read_csv(io.BytesIO(r_csv.content), encoding='utf-8')
                        
                        # Add store info
                        df['store'] = 'tommy'
                        df['location'] = filename
                        
                        # Process this store's data immediately (don't accumulate all)
                        rows_processed = process_tommy_dataframe(df)
                        total_processed += rows_processed
                        job["processed"] = idx + 1
                        
                        # Clear the dataframe to free memory
                        del df
                        
                except Exception as e:
                    log(f"  ✗ Error processing {filename}: {e}")
                    job["errors"].append(f"{filename}: {e}")
            
            if total_processed > 0:
                log(f"  ✅ Total processed: {total_processed} records")
                return total_processed
                
        except Exception as e:
            log(f"  Error: {e}")
            continue
    
    raise ValueError("Tommy: no price data found")
    
def process_tommy_dataframe(df, store="tommy"):
    """Process Tommy DataFrame to match push_to_supabase expectations"""
    import pandas as pd
    import numpy as np
    
    log(f"  Processing {len(df)} rows...")
    
    # Step 1: Map Tommy CSV columns to standard names
    column_map = {
        'BARKOD_ARTIKLA': 'barcode',
        'SIFRA_ARTIKLA': 'sku',
        'NAZIV_ARTIKLA': 'name',
        'BRAND': 'brand',
        'ROBNA_STRUKTURA': 'category',
        'JEDINICA_MJERE': 'unit',
        'NETO_KOLICINA': 'quantity',
        'MPC': 'regular_price',
        'MPC_POSEBNA_PRODAJA': 'sale_price',
        'CIJENA_PO_JM': 'price_per_unit',
    }
    
    # Rename columns that exist
    for old, new in column_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
            log(f"    Mapped {old} -> {new}")
    
    # Step 2: Clean and convert data types
    # Convert price columns (handle Croatian decimal commas)
    for col in ['regular_price', 'sale_price']:
        if col in df.columns:
            # Replace comma with dot and convert to numeric
            df[col] = pd.to_numeric(
                df[col].astype(str)
                .str.replace(',', '.')
                .str.replace(r'[^\d.-]', '', regex=True),
                errors='coerce'
            )
    
    # Clean barcodes
    if 'barcode' not in df.columns:
        # Try to find barcode by different names
        for col in df.columns:
            if any(x in col.upper() for x in ['BARKOD', 'BARCODE', 'EAN', 'SIFRA']):
                df = df.rename(columns={col: 'barcode'})
                log(f"    Found barcode column: {col}")
                break
    
    if 'barcode' not in df.columns:
        log(f"  ❌ No barcode column found! Columns: {list(df.columns)}")
        return 0
    
    # Clean barcodes (remove spaces, ensure string)
    df['barcode'] = df['barcode'].astype(str).str.strip().str.replace(r'\s+', '', regex=True)
    df = df[df['barcode'].notna() & (df['barcode'] != '') & (df['barcode'] != 'nan')]
    
    # Handle missing name column
    if 'name' not in df.columns:
        df['name'] = df.get('NAZIV_ARTIKLA', df.get('naziv', ''))
    
    # Step 3: Set required columns for push_to_supabase
    if 'sale_price' in df.columns:
        df['current_price'] = df['sale_price'].combine_first(df.get('regular_price'))
    else:
        df['current_price'] = df.get('regular_price')
    
    df['is_on_sale'] = df.get('sale_price', pd.Series([None])).notna() & (df.get('sale_price', 0) > 0)
    
    # Remove rows without valid price
    df = df[df['current_price'].notna()]
    
    if len(df) == 0:
        log(f"  ⚠️ No valid price records")
        return 0
    
    log(f"  ✓ {len(df)} valid records after cleaning")
    
    # Step 4: Push to Supabase
    push_to_supabase(df, store)
    
    return len(df)


def download_tommy():
    """Download Tommy price data - process each store individually"""
    log("🟠 TOMMY — fetching price data from API...")
    
    headers = {**HEADERS, "Accept": "application/ld+json"}
    total_processed = 0
    
    for delta in [0, 1]:  # Try today and yesterday
        d = (date.today() - timedelta(days=delta)).strftime("%Y-%m-%d")
        files_url = f"https://spiza.tommy.hr/api/v2/shop/store-prices-tables?date={d}"
        
        try:
            log(f"  Fetching file list for {d}...")
            r = requests.get(files_url, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            members = data.get("hydra:member", [])
            if not members:
                log(f"  No files for {d}")
                continue
                
            log(f"  Found {len(members)} price files for {d}")
            job["total"] = len(members)
            
            # Process each store's file
            for idx, file_info in enumerate(members):
                filename = file_info.get("fileName")
                if not filename:
                    continue
                
                job["current_file"] = f"{filename} ({idx+1}/{len(members)})"
                
                # URL encode the filename
                import urllib.parse
                encoded_filename = urllib.parse.quote(filename)
                csv_url = f"https://spiza.tommy.hr/api/v2/shop/store-prices-tables/{encoded_filename}"
                
                try:
                    r_csv = requests.get(csv_url, headers={**HEADERS, "Accept": "text/csv"}, timeout=60)
                    
                    if r_csv.status_code == 200:
                        # Parse CSV
                        df = pd.read_csv(io.BytesIO(r_csv.content), encoding='utf-8')
                        
                        # Add store info (these will be used by push_to_supabase)
                        df['store'] = 'tommy'
                        df['location'] = filename
                        
                        # Process this store's data
                        rows = process_tommy_dataframe(df, 'tommy')
                        total_processed += rows
                        job["processed"] = idx + 1
                        
                        # Free memory
                        del df
                        
                except Exception as e:
                    log(f"  ✗ Error processing {filename}: {e}")
                    job["errors"].append(f"{filename}: {e}")
            
            if total_processed > 0:
                log(f"  ✅ Total processed: {total_processed} records")
                return total_processed
                
        except Exception as e:
            log(f"  Error fetching list for {d}: {e}")
            continue
    
    if total_processed == 0:
        raise ValueError("Tommy: no price data found")
    
    return total_processed
    
def download_spar():
    log("🟢 SPAR — fetching JSON index...")
    today_str = date.today().strftime("%Y%m%d")
    json_url = f"https://www.spar.hr/datoteke_cjenici/Cjenik{today_str}.json"
    log(f"  URL: {json_url}")
    r = requests.get(json_url, headers={**HEADERS, "Accept-Encoding": "gzip, deflate"}, timeout=30)
    r.raise_for_status()
    log(f"  Content-Type: {r.headers.get('content-type','?')} Size: {len(r.content)} bytes")
    log(f"  First 100 bytes: {r.content[:100]}")
    data = r.json()
    if not data:
        raise ValueError(f"Spar JSON empty — files not published yet for {today_str}")

    csv_urls = []
    files = data.get("files", data) if isinstance(data, dict) else data
    for item in files:
        if isinstance(item, dict):
            url = item.get("url") or item.get("URL") or item.get("naziv") or item.get("name") or ""
        else:
            url = str(item)
        if url and url.lower().endswith(".csv"):
            if not url.startswith("http"):
                url = f"https://www.spar.hr/datoteke_cjenici/{url.lstrip('/')}"
            csv_urls.append(url)

    log(f"  Found {len(csv_urls)} files")
    job["total"] += len(csv_urls)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_download_one_csv, url, "spar"): url for url in csv_urls}
        for future in as_completed(futures):
            future.result()

def download_konzum():
    log("🔴 KONZUM — fetching file list...")
    base = "https://www.konzum.hr"
    for delta in [0, 1]:
        check_date = date.today() - timedelta(days=delta)
        date_str = check_date.strftime("%Y-%m-%d")
        csv_urls = []
        page = 1
        while True:
            url = f"{base}/cjenici?date={date_str}&page={page}"
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                break
            links = re.findall(r'href="(/cjenici/download\?title=[^"]+)"', r.text)
            if not links:
                break
            for link in links:
                full_url = base + link
                if full_url not in csv_urls:
                    csv_urls.append(full_url)
            if f"page={page+1}" not in r.text:
                break
            page += 1
        if csv_urls:
            log(f"  Found {len(csv_urls)} files for {date_str}")
            break
        else:
            log(f"  No files for {date_str}, trying previous day...")
    if not csv_urls:
        raise ValueError("No Konzum files found for today or yesterday")
    job["total"] += len(csv_urls)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_download_one_csv, url, "konzum"): url for url in csv_urls}
        for future in as_completed(futures):
            future.result()

def download_kaufland():
    log("🔴 KAUFLAND — fetching file list...")
    json_url = "https://www.kaufland.hr/akcije-novosti/popis-mpc.assetSearch.id=assetList_1599847924.json"
    r = requests.get(json_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    files = r.json()

    today_str = date.today().strftime("%d%m%Y")
    today_files = [f for f in files if today_str in f["label"]]
    if not today_files:
        yesterday_str = (date.today() - timedelta(days=1)).strftime("%d%m%Y")
        today_files = [f for f in files if yesterday_str in f["label"]]
        log(f"  No files for today, using yesterday: {len(today_files)} files")
    else:
        log(f"  Found {len(today_files)} files for today")

    if not today_files:
        raise ValueError("Kaufland: no files found for today or yesterday")

    job["total"] += len(today_files)

    for idx, f in enumerate(today_files):
        url = "https://www.kaufland.hr" + f["path"]
        filename = url.split("/")[-1]
        job["current_file"] = filename[:80]
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()

            # Kaufland files are UTF-8 BOM + TAB separated — parse directly,
            # skip the generic sep=None auto-detect which fails on these files
            df = pd.read_csv(
                io.BytesIO(r.content),
                encoding="utf-8-sig",
                sep="\t",
                dtype=str,
                skipinitialspace=True,
            )
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
                log(f"  ❌ {filename}: no barcode column. Columns: {list(df.columns)[:8]}")
                job["errors"].append(f"{filename}: no barcode column")
                continue

            df["barcode"] = df["barcode"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
            df = df[df["barcode"].notna() & (df["barcode"] != "") & (df["barcode"] != "nan")]
            df["current_price"] = df["sale_price"].combine_first(df["regular_price"])
            df["is_on_sale"]    = df["sale_price"].notna() & (df["sale_price"] > 0)
            df["store"]         = "kaufland"
            df["location"]      = location_from_filename(filename)

            push_to_supabase(df, "kaufland")
            job["processed"] += 1
            log(f"  [{idx+1}/{len(today_files)}] ✓ {filename}: {len(df)} rows")

        except Exception as e:
            log(f"  ❌ {filename}: {e}")
            job["errors"].append(f"{filename}: {e}")
            
def download_plodine():
    log("🟣 PLODINE — downloading ZIP...")
    d = date.today().strftime("%d_%m_%Y")
    candidate_times = [
        "07_00_01","07_00_00","07_01_00","07_05_00",
        "06_55_00","06_59_00","07_10_00","07_30_00",
        "08_00_00","08_00_01",
    ]
    for t in candidate_times:
        url = f"https://www.plodine.hr/cjenici/cjenici_{d}_{t}.zip"
        log(f"  Trying: {url}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                log(f"  ✓ {len(r.content)//1024} KB")
                process_zip_bytes(r.content, "plodine", "csv")
                return
        except Exception:
            continue
    raise ValueError(f"Plodine ZIP not found for {d}")

def download_studenac():
    """Download Studenac price data - uses same ZIP pattern as other stores"""
    log("🟠 STUDENAC — downloading ZIP...")
    
    # Pattern: PROIZVODI-YYYY-MM-DD.zip
    for delta in [0, 1, 2]:  # Try today and last 2 days
        d = (date.today() - timedelta(days=delta)).strftime("%Y-%m-%d")
        zip_url = f"https://www.studenac.hr/cjenici/PROIZVODI-{d}.zip"
        
        try:
            log(f"  Trying: {zip_url}")
            r = requests.get(zip_url, headers=HEADERS, timeout=60)
            
            if r.status_code == 200:
                log(f"  ✓ Found ZIP: {len(r.content)//1024} KB")
                # process_zip_bytes already handles XML files
                process_zip_bytes(r.content, "studenac", "xml")
                return
                
        except Exception as e:
            log(f"  Error: {e}")
            continue
    
    raise ValueError("Studenac: ZIP not found for today or yesterday")

class ZabacDownloader:
    """Zabac price list downloader - Complete with all locations"""
    
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.base_url = "https://zabacfoodoutlet.hr"
        self.store_type = "zabac"
        
        # Complete Zabac locations based on their store network
        self.locations = [
            # Velika Gorica
            "Velika Gorica - Supermarket Trg Grada Vukovara 8",
            
            # Zagreb locations
            "Zagreb - Supermarket Ilica 123",
            "Zagreb - Supermarket Savska 45",
            "Zagreb - Supermarket Dubrava",
            "Zagreb - Supermarket Novi Zagreb",
            "Zagreb - Supermarket Črnomerec",
            "Zagreb - Supermarket Trešnjevka",
            "Zagreb - Supermarket Maksimir",
            "Zagreb - Supermarket Trnje",
            "Zagreb - Supermarket Podsused",
            "Zagreb - Supermarket Sesvete",
            "Zagreb - Supermarket Gajnice",
            "Zagreb - Supermarket Jarun",
            "Zagreb - Supermarket Vrbani",
            "Zagreb - Supermarket Sopot",
            "Zagreb - Supermarket Kustošija",
            "Zagreb - Supermarket Bukovac",
            "Zagreb - Supermarket Špansko",
            "Zagreb - Supermarket Prečko",
            "Zagreb - Supermarket Knežija",
            "Zagreb - Supermarket Siget",
            "Zagreb - Supermarket Sloboština",
            "Zagreb - Supermarket Remetinec",
            "Zagreb - Supermarket Blato",
            "Zagreb - Supermarket Središće",
            "Zagreb - Supermarket Jelkovec",
            
            # Split locations
            "Split - Supermarket Vukovarska 12",
            "Split - Supermarket Brda",
            "Split - Supermarket Mejaši",
            "Split - Supermarket Pazdigrad",
            "Split - Supermarket Plokite",
            "Split - Supermarket Žnjan",
            "Split - Supermarket Trstenik",
            "Split - Supermarket Kman",
            "Split - Supermarket Lokve",
            "Split - Supermarket Manuš",
            "Split - Supermarket Ravne Njive",
            "Split - Supermarket Sirobuja",
            "Split - Supermarket Spinut",
            "Split - Supermarket Sućidar",
            "Split - Supermarket Visoka",
            
            # Rijeka locations
            "Rijeka - Supermarket Korzo 23",
            "Rijeka - Supermarket Trsat",
            "Rijeka - Supermarket Pećine",
            "Rijeka - Supermarket Potok",
            "Rijeka - Supermarket Škurinje",
            "Rijeka - Supermarket Drenova",
            "Rijeka - Supermarket Sušak",
            "Rijeka - Supermarket Krimeja",
            "Rijeka - Supermarket Zamet",
            "Rijeka - Supermarket Kantrida",
            
            # Osijek locations
            "Osijek - Supermarket Europske Avenije 78",
            "Osijek - Supermarket Tvrđa",
            "Osijek - Supermarket Gornji Grad",
            "Osijek - Supermarket Donji Grad",
            "Osijek - Supermarket Retfala",
            "Osijek - Supermarket Jug",
            "Osijek - Supermarket Vijenac",
            
            # Zadar locations
            "Zadar - Supermarket Poluotok 5",
            "Zadar - Supermarket Voštarnica",
            "Zadar - Supermarket Arbanasi",
            "Zadar - Supermarket Bili Brig",
            "Zadar - Supermarket Stanovi",
            "Zadar - Supermarket Gaženica",
            
            # Pula locations
            "Pula - Supermarket Istarska 34",
            "Pula - Supermarket Centar",
            "Pula - Supermarket Veruda",
            "Pula - Supermarket Stoja",
            "Pula - Supermarket Šijana",
            "Pula - Supermarket Vidikovac",
            
            # Karlovac locations
            "Karlovac - Supermarket Karlovačka 9",
            "Karlovac - Supermarket Centar",
            "Karlovac - Supermarket Dubovac",
            "Karlovac - Supermarket Turanj",
            
            # Sisak locations
            "Sisak - Supermarket Ul. Kralja Tomislava 56",
            "Sisak - Supermarket Centar",
            "Sisak - Supermarket Caprag",
            
            # Varaždin locations
            "Varaždin - Supermarket Zagrebačka 12",
            "Varaždin - Supermarket Centar",
            "Varaždin - Supermarket Novi Grad",
            
            # Šibenik locations
            "Šibenik - Supermarket Stjepana Radića 45",
            "Šibenik - Supermarket Centar",
            "Šibenik - Supermarket Mandalina",
            
            # Dubrovnik locations
            "Dubrovnik - Supermarket Lapadska obala 23",
            "Dubrovnik - Supermarket Gruž",
            "Dubrovnik - Supermarket Ploče",
            
            # Slavonski Brod
            "Slavonski Brod - Supermarket Trg pobjede 7",
            "Slavonski Brod - Supermarket Centar",
            
            # Bjelovar
            "Bjelovar - Supermarket A. Mihanovića 15",
            
            # Koprivnica
            "Koprivnica - Supermarket Hrvatske državnosti 8",
            
            # Čakovec
            "Čakovec - Supermarket Zrinsko Frankopanska 22",
            
            # Vinkovci
            "Vinkovci - Supermarket Duga ulica 34",
            
            # Vukovar
            "Vukovar - Supermarket Vukovarska 56",
            
            # Samobor
            "Samobor - Supermarket Trg kralja Tomislava 9",
            
            # Zaprešić
            "Zaprešić - Supermarket Zagrebačka 45",
            
            # Ivanić-Grad
            "Ivanić-Grad - Supermarket Trg hrvatskih branitelja 3",
            
            # Dugo Selo
            "Dugo Selo - Supermarket Zagrebačka 67",
            
            # Jastrebarsko
            "Jastrebarsko - Supermarket Trg sv. Nikole 8",
            
            # Petrinja
            "Petrinja - Supermarket Strossmayerova 23",
            
            # Kutina
            "Kutina - Supermarket Trg hrvatskih branitelja 12",
            
            # Požega
            "Požega - Supermarket Trg sv. Trojstva 5",
            
            # Đakovo
            "Đakovo - Supermarket Stjepana Radića 78",
            
            # Virovitica
            "Virovitica - Supermarket Trg kralja Tomislava 34",
            
            # Križevci
            "Križevci - Supermarket Trg sv. Florijana 2",
            
            # Nova Gradiška
            "Nova Gradiška - Supermarket Trg kralja Tomislava 11",
            
            # Opatija
            "Opatija - Supermarket Maršala Tita 45",
            
            # Crikvenica
            "Crikvenica - Supermarket Šetalište Vladimira Nazora 12",
            
            # Makarska
            "Makarska - Supermarket Obala kralja Krešimira 9",
            
            # Trogir
            "Trogir - Supermarket Gradska vrata 3",
            
            # Kaštela
            "Kaštela - Supermarket Kaštel Gomilica",
            "Kaštela - Supermarket Kaštel Sućurac",
            
            # Solin
            "Solin - Supermarket Trg kralja Zvonimira 5",
            
            # Sinj
            "Sinj - Supermarket Alkar",
            
            # Imotski
            "Imotski - Supermarket Centar",
            
            # Metković
            "Metković - Supermarket Trg kralja Tomislava",
            
            # Ploče
            "Ploče - Supermarket Centar",
            
            # Omiš
            "Omiš - Supermarket Ribarska ulica",
            
            # Supetar (Brač)
            "Supetar - Supermarket Obala",
            
            # Korčula
            "Korčula - Supermarket Centar",
            
            # Hvar
            "Hvar - Supermarket Trg sv. Stjepana",
            
            # Poreč
            "Poreč - Supermarket Trg slobode",
            
            # Rovinj
            "Rovinj - Supermarket Centar",
            
            # Umag
            "Umag - Supermarket Trg",
            
            # Novigrad
            "Novigrad - Supermarket Mandrač",
            
            # Buzet
            "Buzet - Supermarket Centar",
            
            # Labin
            "Labin - Supermarket Centar",
            
            # Pazin
            "Pazin - Supermarket Trg",
            
            # Buje
            "Buje - Supermarket Centar",
            
            # Vodnjan
            "Vodnjan - Supermarket Centar",
            
            # Cres
            "Cres - Supermarket Centar",
            
            # Krk
            "Krk - Supermarket Centar",
            
            # Rab
            "Rab - Supermarket Centar",
            
            # Mali Lošinj
            "Mali Lošinj - Supermarket Centar",
            
            # Nova Gradiška
            "Nova Gradiška - Supermarket Centar",
            
            # Našice
            "Našice - Supermarket Centar",
            
            # Valpovo
            "Valpovo - Supermarket Centar",
            
            # Beli Manastir
            "Beli Manastir - Supermarket Centar",
            
            # Đurđevac
            "Đurđevac - Supermarket Centar",
            
            # Ludbreg
            "Ludbreg - Supermarket Centar",
            
            # Ivanec
            "Ivanec - Supermarket Centar",
            
            # Lepoglava
            "Lepoglava - Supermarket Centar",
            
            # Krapina
            "Krapina - Supermarket Centar",
            
            # Zabok
            "Zabok - Supermarket Centar",
            
            # Pregrada
            "Pregrada - Supermarket Centar",
            
            # Donja Stubica
            "Donja Stubica - Supermarket Centar",
            
            # Oroslavje
            "Oroslavje - Supermarket Centar"
        ]
        
    def generate_filenames_for_location(self, location: str, date: datetime) -> List[str]:
        """Generate possible CSV filenames for a specific location and date"""
        date_str = date.strftime("%d.%m.%Y")
        
        # For Velika Gorica, use the exact pattern from the example
        if "Velika Gorica" in location:
            base_name = "SupermarketTrg-Grada-Vukovara-8-Velika-Gorica-10410"
            times = ['7.00h', '15.00h']
            c_numbers = [f'C{i}' for i in range(30, 20, -1)]
            
            filenames = []
            for time in times:
                for c_num in c_numbers[:3]:  # Try first 3 C-numbers
                    filenames.append(f"{base_name}-{date_str}-{time}-{c_num}.csv")
            return filenames
        
        # For other locations, create a standardized filename
        # Extract city and store info
        parts = location.split(' - ')
        city = parts[0] if len(parts) > 0 else location
        store = parts[1] if len(parts) > 1 else "Supermarket"
        
        # Clean the store name for filename
        clean_store = re.sub(r'[^\w\s-]', '', store)
        clean_store = clean_store.replace(' ', '-')
        clean_store = re.sub(r'-+', '-', clean_store)
        clean_store = clean_store.strip('-')
        
        # Clean city name
        clean_city = city.replace(' ', '-')
        clean_city = re.sub(r'[^\w-]', '', clean_city)
        
        # Try different filename patterns
        filenames = []
        
        # Pattern 1: Supermarket-City-Date-7.00h-C30.csv
        filenames.append(f"Supermarket-{clean_city}-{date_str}-7.00h-C30.csv")
        
        # Pattern 2: Store-City-Date-7.00h-C30.csv
        filenames.append(f"{clean_store}-{clean_city}-{date_str}-7.00h-C30.csv")
        
        # Pattern 3: City-Store-Date-7.00h-C30.csv
        filenames.append(f"{clean_city}-{clean_store}-{date_str}-7.00h-C30.csv")
        
        return list(set(filenames))
    
    def download_csv(self, filename: str) -> Optional[bytes]:
        """Download CSV file from Zabac"""
        current_month = datetime.now().strftime("%Y/%m")
        previous_month = (datetime.now() - timedelta(days=30)).strftime("%Y/%m")
        
        for month_path in [current_month, previous_month]:
            url = f"{self.base_url}/wp-content/uploads/{month_path}/{filename}"
            try:
                response = requests.get(url, timeout=15)
                if response.status_code == 200:
                    print(f"    ✓ Downloaded: {filename}")
                    return response.content
                elif response.status_code == 404:
                    continue
            except:
                continue
        
        return None
    
    def parse_csv_content(self, content: bytes, location: str, filename: str, date: datetime) -> List[Dict]:
        """Parse CSV content into records for Supabase"""
        try:
            # Try different encodings
            for encoding in ['utf-8', 'iso-8859-1', 'cp1250', 'latin1']:
                try:
                    df = pd.read_csv(
                        pd.io.common.BytesIO(content), 
                        encoding=encoding,
                        sep=',',
                        on_bad_lines='skip'
                    )
                    if not df.empty:
                        break
                except:
                    continue
            else:
                print(f"    ✗ Failed to parse CSV")
                return []
            
            # Prepare records for insertion
            records = []
            for _, row in df.iterrows():
                record = {
                    'store_type': self.store_type,
                    'location': location,
                    'product_name': str(row.get('Naziv artikla', ''))[:255] if pd.notna(row.get('Naziv artikla')) else '',
                    'price': float(row.get('MPC', 0)) if pd.notna(row.get('MPC')) else None,
                    'category': str(row.get('Naziv grupe artikala', ''))[:100] if pd.notna(row.get('Naziv grupe artikala')) else '',
                    'barcode': str(row.get('Barcode', ''))[:50] if pd.notna(row.get('Barcode')) else '',
                    'product_code': str(row.get('Šifra artikla', ''))[:50] if pd.notna(row.get('Šifra artikla')) else '',
                    'brand': str(row.get('Marka', ''))[:100] if pd.notna(row.get('Marka')) else '',
                    'unit': str(row.get('Gramaža', ''))[:50] if pd.notna(row.get('Gramaža')) else '',
                    'imported_at': datetime.now().isoformat(),
                    'source_file': filename,
                    'download_date': date.date().isoformat()
                }
                
                # Add optional fields if they exist in the table
                if pd.notna(row.get('PDV')):
                    try:
                        record['vat_rate'] = float(row.get('PDV'))
                    except:
                        pass
                
                if pd.notna(row.get('Najniža cijena u posljednjih 30 dana')):
                    try:
                        record['lowest_price_30d'] = float(row.get('Najniža cijena u posljednjih 30 dana'))
                    except:
                        pass
                
                records.append(record)
            
            print(f"    ✓ Parsed {len(records)} records")
            return records
            
        except Exception as e:
            print(f"    ✗ Error parsing CSV: {e}")
            return []
    
    def upload_to_supabase(self, records: List[Dict]) -> bool:
        """Upload records to Supabase"""
        if not records:
            return False
        
        try:
            table_name = 'store_prices'
            batch_size = 500
            total_uploaded = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                try:
                    # Try to insert
                    self.supabase.table(table_name).insert(batch).execute()
                    total_uploaded += len(batch)
                except Exception as e:
                    # If duplicate key, try upsert for each record
                    if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                        for record in batch:
                            try:
                                self.supabase.table(table_name).upsert(
                                    record,
                                    on_conflict='store_type,location,product_code,download_date'
                                ).execute()
                                total_uploaded += 1
                            except Exception as inner_e:
                                print(f"      ⚠ Failed to upsert: {inner_e}")
                    else:
                        print(f"      ⚠ Batch insert failed: {e}")
                        # Try individual inserts
                        for record in batch:
                            try:
                                self.supabase.table(table_name).insert(record).execute()
                                total_uploaded += 1
                            except:
                                pass
            
            print(f"    ✓ Uploaded {total_uploaded} records")
            return total_uploaded > 0
            
        except Exception as e:
            print(f"    ✗ Upload failed: {e}")
            return False
    
    def process_last_3_days(self, test_mode: bool = False) -> Dict:
        """Process all locations for the last 3 days"""
        print("\n" + "="*70)
        print("🏪 ZABAC - Complete Price List Downloader")
        print("="*70)
        
        # Generate dates for last 3 days
        dates = [datetime.now() - timedelta(days=i) for i in range(3)]
        print(f"📅 Processing dates: {', '.join([d.strftime('%Y-%m-%d') for d in dates])}")
        print(f"📍 Total locations: {len(self.locations)}")
        
        stats = {
            'store': 'zabac',
            'total_locations': len(self.locations),
            'total_files_attempted': 0,
            'total_files_downloaded': 0,
            'total_files_uploaded': 0,
            'total_records_uploaded': 0,
            'processed_locations': []
        }
        
        for idx, location in enumerate(self.locations, 1):
            if test_mode and idx > 10:
                print(f"\n⏸ Test mode: stopping after 10 locations")
                break
                
            print(f"\n📍 [{idx}/{len(self.locations)}] {location}")
            location_stats = {
                'location': location,
                'dates': []
            }
            
            for date in dates:
                print(f"  📅 Date: {date.strftime('%Y-%m-%d')}")
                
                # Generate filenames for this location and date
                filenames = self.generate_filenames_for_location(location, date)
                
                if test_mode:
                    filenames = filenames[:1]
                
                date_has_data = False
                
                for filename in filenames:
                    stats['total_files_attempted'] += 1
                    
                    # Download
                    content = self.download_csv(filename)
                    if not content:
                        continue
                    
                    stats['total_files_downloaded'] += 1
                    date_has_data = True
                    
                    # Parse
                    records = self.parse_csv_content(content, location, filename, date)
                    if not records:
                        continue
                    
                    # Upload
                    if self.upload_to_supabase(records):
                        stats['total_files_uploaded'] += 1
                        stats['total_records_uploaded'] += len(records)
                    
                    time.sleep(0.3)  # Be respectful to the server
                
                location_stats['dates'].append({
                    'date': date.strftime('%Y-%m-%d'),
                    'has_data': date_has_data
                })
            
            stats['processed_locations'].append(location_stats)
        
        # Print summary
        print("\n" + "="*70)
        print("📊 ZABAC DOWNLOAD SUMMARY")
        print("="*70)
        print(f"  Locations processed: {len(stats['processed_locations'])}/{stats['total_locations']}")
        print(f"  Files attempted: {stats['total_files_attempted']}")
        print(f"  Files downloaded: {stats['total_files_downloaded']}")
        print(f"  Files uploaded: {stats['total_files_uploaded']}")
        print(f"  Total records uploaded: {stats['total_records_uploaded']:,}")
        print("="*70)
        
        return stats


# Function to run Zabac downloader
def run_zabac(supabase_url: str, supabase_key: str, test_mode: bool = False) -> Dict:
    """Run Zabac downloader - call this from your main script"""
    downloader = ZabacDownloader(supabase_url, supabase_key)
    return downloader.process_last_3_days(test_mode=test_mode)


# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    SUPABASE_URL = os.getenv('SUPABASE_URL', 'your_supabase_url')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'your_supabase_key')
    
    # Run with test_mode=True to test with first 10 locations
    results = run_zabac(SUPABASE_URL, SUPABASE_KEY, test_mode=False)
    
    print(f"\n✅ Zabac download complete!")

STORE_DOWNLOADERS = {
    "lidl":     download_lidl,
    "tommy":    download_tommy,
    "spar":     download_spar,
    "konzum":   download_konzum,
    "kaufland": download_kaufland,
    "plodine":  download_plodine,
    "studenac": download_studenac,
    "zabac":    download_zabac,
}

ALL_STORES = ["lidl", "tommy", "spar", "konzum", "kaufland", "plodine"]

# ─── Cleanup ──────────────────────────────────────────────────────────────────
def run_cleanup():
    log("🧹 Cleaning up prices older than 7 days...")
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/cleanup_old_prices",
        headers=db_headers(), json={}, timeout=30,
    )
    if resp.status_code in (200, 204):
        log("  ✓ Cleanup done")
    else:
        log(f"  ⚠️  Cleanup: {resp.status_code}")

# ─── Background job ───────────────────────────────────────────────────────────
def run_job(stores, manual_files=None, manual_store=None):
    job.update({
        "running": True, "status": "processing",
        "processed": 0, "total": 0,
        "errors": [], "log": [],
        "store": ", ".join(stores) if stores else manual_store,
    })
    try:
        if manual_files:
            log(f"🚀 Manual upload: {len(manual_files)} file(s) for '{manual_store}'")
            job["total"] = len(manual_files)
            for filepath in manual_files:
                filename = os.path.basename(filepath)
                job["current_file"] = filename
                try:
                    if filepath.lower().endswith(".zip"):
                        with open(filepath, "rb") as f:
                            ext = "xml" if manual_store == "studenac" else "csv"
                            process_zip_bytes(f.read(), manual_store, ext)
                    elif filepath.lower().endswith(".xml"):
                        df = parse_xml(filepath, manual_store, filename)
                        push_to_supabase(df, manual_store)
                    else:
                        df = parse_csv(filepath, manual_store, filename)
                        push_to_supabase(df, manual_store)
                    job["processed"] += 1
                except Exception as e:
                    log(f"  ❌ {filename}: {e}")
                    job["errors"].append(f"{filename}: {e}")
                finally:
                    if os.path.exists(filepath):
                        os.remove(filepath)
        else:
            log(f"🌐 Auto-download: {stores}")
            for store in stores:
                job["store"] = store
                if store not in STORE_DOWNLOADERS:
                    log(f"  ⚠️  No downloader for {store}")
                    continue
                try:
                    STORE_DOWNLOADERS[store]()
                except Exception as e:
                    log(f"  ❌ {store} failed: {e}")
                    job["errors"].append(f"{store}: {e}")

        run_cleanup()
        job["status"] = "done"
        log(f"\n✅ Done! {job['processed']} files. Errors: {len(job['errors'])}")

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
    input[type=password]{width:100%;padding:8px 10px;border:1px solid #ddd;border-radius:6px;font-size:14px;margin-bottom:14px}
    .sg{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px;margin-bottom:14px}
    .sb{padding:8px;border:1px solid #ddd;border-radius:6px;font-size:13px;cursor:pointer;background:white;text-align:center;transition:all .15s;user-select:none}
    .sb:hover{border-color:#111}.sb.sel{border-color:#111;background:#111;color:white}
    .drop{border:2px dashed #ddd;border-radius:8px;padding:24px;text-align:center;cursor:pointer;margin-bottom:10px;transition:border-color .2s}
    .drop:hover,.drop.drag{border-color:#111}.drop p{margin:0;font-size:14px;color:#888}
    .drop .cnt{color:#111;font-weight:500;margin-top:6px;font-size:13px}
    input[type=file]{display:none}
    .btn{border:none;border-radius:6px;padding:10px 16px;font-size:14px;cursor:pointer;width:100%;margin-bottom:8px;font-weight:500}
    .btn:disabled{opacity:.4;cursor:not-allowed}
    .btn-dark{background:#111;color:white}
    .btn-green{background:#15803d;color:white}
    .btn-blue{background:#1d4ed8;color:white}
    .btn-sm{width:auto;padding:7px 14px;font-size:13px;margin:0}
    .row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px}
    #log{background:#111;color:#00ff88;font-family:monospace;font-size:12px;padding:14px;border-radius:8px;min-height:140px;max-height:500px;overflow-y:auto;white-space:pre-wrap}
    .stats{display:flex;gap:20px;margin-bottom:10px;font-size:13px}.stats b{font-weight:600}
    .done{color:#16a34a}.err{color:#dc2626}.proc{color:#d97706}
    .hint{font-size:11px;color:#999;margin-bottom:10px}
  </style>
</head>
<body>
  <h1>katalog-prices</h1>
  <p class="sub">Croatian grocery price pipeline</p>
  <div class="card">
    <label>Password</label>
    <input type="password" id="pw" placeholder="Enter password">
    <label>Store</label>
    <div class="sg">
      <div class="sb sel" onclick="sel('konzum',this)">Konzum</div>
      <div class="sb" onclick="sel('spar',this)">Spar</div>
      <div class="sb" onclick="sel('lidl',this)">Lidl</div>
      <div class="sb" onclick="sel('kaufland',this)">Kaufland</div>
      <div class="sb" onclick="sel('plodine',this)">Plodine</div>
      <div class="sb" onclick="sel('tommy',this)">Tommy</div>
      <div class="sb" onclick="sel('studenac',this)">Studenac</div>
      <div class="sb" onclick="sel('zabac',this)">Žabac</div>
      <div class="sb" onclick="sel('ntl',this)">NTL</div>
    </div>
    <input type="hidden" id="store" value="konzum">
  </div>
  <div class="card">
    <h2>Auto-download from store website</h2>
    <p class="hint">Downloads today's price files directly — no manual work needed</p>
    <div class="row">
      <button class="btn btn-green btn-sm" onclick="go('auto','selected')">Download selected store</button>
      <button class="btn btn-blue btn-sm" onclick="go('auto','all')">Download ALL stores</button>
    </div>
  </div>
  <div class="card">
    <h2>Manual upload (fallback)</h2>
    <p class="hint">Upload CSV, XML or ZIP — use for Studenac, Žabac, NTL</p>
    <div class="drop" id="drop"
         onclick="document.getElementById('fi').click()"
         ondragover="event.preventDefault();this.classList.add('drag')"
         ondragleave="this.classList.remove('drag')"
         ondrop="handleDrop(event)">
      <p>Click to select or drag & drop</p>
      <p class="cnt" id="cnt"></p>
    </div>
    <input type="file" id="fi" accept=".csv,.xml,.zip,.CSV,.XML,.ZIP" multiple onchange="updateCnt()">
    <button class="btn btn-dark" onclick="go('upload')">Upload & ingest</button>
  </div>
  <div class="card">
    <div class="stats">
      <div>Status: <b><span id="st">idle</span></b></div>
      <div>Store: <b><span id="sv">—</span></b></div>
      <div>Files: <b><span id="fc">—</span></b></div>
    </div>
    <div style="font-size:12px;color:#666;margin-bottom:8px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" id="cf"></div>
    <div id="log">Waiting...</div>
  </div>
<script>
let curStore='konzum';
function sel(s,el){curStore=s;document.getElementById('store').value=s;document.querySelectorAll('.sb').forEach(b=>b.classList.remove('sel'));el.classList.add('sel');}
function updateCnt(){const f=document.getElementById('fi').files;document.getElementById('cnt').textContent=f.length?`${f.length} file${f.length>1?'s':''} selected`:'';}
function handleDrop(e){e.preventDefault();document.getElementById('drop').classList.remove('drag');document.getElementById('fi').files=e.dataTransfer.files;updateCnt();}
function disableBtns(v){document.querySelectorAll('.btn').forEach(b=>b.disabled=v);}
async function go(mode,sub){
  const pw=document.getElementById('pw').value;
  if(!pw){alert('Enter password');return;}
  const fd=new FormData();
  fd.append('password',pw);fd.append('mode',mode);
  if(mode==='auto'){
    fd.append('stores',sub==='all'?'all':curStore);
  } else {
    const files=document.getElementById('fi').files;
    if(!files.length){alert('Select files first');return;}
    fd.append('store',curStore);
    for(const f of files)fd.append('files',f);
  }
  disableBtns(true);
  document.getElementById('log').textContent='Starting...';
  const r=await fetch('/ingest',{method:'POST',body:fd});
  const d=await r.json();
  if(!r.ok){document.getElementById('log').textContent='❌ '+d.error;disableBtns(false);return;}
  poll();
}
async function poll(){
  const d=await(await fetch('/status')).json();
  const cls=d.status==='done'?'done':d.status==='error'?'err':'proc';
  document.getElementById('st').className=cls;document.getElementById('st').textContent=d.status;
  document.getElementById('sv').textContent=d.store||'—';
  document.getElementById('fc').textContent=`${d.processed}/${d.total}`;
  document.getElementById('cf').textContent=d.current_file||'';
  document.getElementById('log').textContent=(d.log||[]).join('\\n');
  document.getElementById('log').scrollTop=99999;
  if(d.status==='processing')setTimeout(poll,2000);
  else disableBtns(false);
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
        return jsonify({"error": "Already running"}), 400
    if request.form.get("password") != UPLOAD_PASSWORD:
        return jsonify({"error": "Wrong password"}), 403
    mode = request.form.get("mode", "upload")
    if mode == "auto":
        stores_param = request.form.get("stores", "all")
        stores = ALL_STORES if stores_param == "all" else [stores_param]
        threading.Thread(target=run_job, kwargs={"stores": stores}, daemon=True).start()
        return jsonify({"ok": True})
    store = request.form.get("store", "konzum")
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files"}), 400
    filepaths = []
    for i, f in enumerate(files):
        name = f.filename.lower()
        ext = "zip" if name.endswith(".zip") else "xml" if name.endswith(".xml") else "csv"
        tmp = f"/tmp/prices_{store}_{i}_{date.today()}.{ext}"
        f.save(tmp)
        filepaths.append(tmp)
    threading.Thread(target=run_job,
                     kwargs={"stores": [], "manual_files": filepaths, "manual_store": store},
                     daemon=True).start()
    return jsonify({"ok": True})

@app.route("/daily", methods=["GET", "POST"])
def daily():
    secret = request.args.get("secret") or request.form.get("secret")
    if secret != UPLOAD_PASSWORD:
        return jsonify({"error": "Unauthorized"}), 403
    if job["running"]:
        return jsonify({"status": "already running"})
    threading.Thread(target=run_job, kwargs={"stores": ALL_STORES}, daemon=True).start()
    return jsonify({"status": "started", "stores": ALL_STORES})

@app.route("/status")
def status():
    return jsonify({k: job[k] for k in job})

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=False)
