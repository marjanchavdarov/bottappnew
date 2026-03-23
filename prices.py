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
from datetime import date, timedelta, datetime
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
    ("sale_price",       ["posebnog oblika", "posebno", "poseb.oblik", "mpc poseb", "posebn", "mpc_posebna", "akcij", "akc", "sale"]),
    ("lowest_30d_price", ["30 dan", "30dana", "30 dana", "najni"]),
    ("anchor_price",     ["sidrena", "anchor"]),
    ("barcode",          ["barkod", "barcode", "ean"]),
    ("category",         ["kategorij", "category", "grupe", "robna", "robna_strukt"]),
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

def download_zabac():
    """Download Zabac price data from all locations for last 3 days"""
    log("🟠 ZABAC — downloading price lists from all locations...")
    
    # Create Supabase client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    base_url = "https://zabacfoodoutlet.hr"
    store_type = "zabac"
    
    # Complete Zabac locations
    locations = [
        "Velika Gorica - Supermarket Trg Grada Vukovara 8",
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
        "Osijek - Supermarket Europske Avenije 78",
        "Osijek - Supermarket Tvrđa",
        "Osijek - Supermarket Gornji Grad",
        "Osijek - Supermarket Donji Grad",
        "Osijek - Supermarket Retfala",
        "Osijek - Supermarket Jug",
        "Osijek - Supermarket Vijenac",
        "Zadar - Supermarket Poluotok 5",
        "Zadar - Supermarket Voštarnica",
        "Zadar - Supermarket Arbanasi",
        "Zadar - Supermarket Bili Brig",
        "Zadar - Supermarket Stanovi",
        "Zadar - Supermarket Gaženica",
        "Pula - Supermarket Istarska 34",
        "Pula - Supermarket Centar",
        "Pula - Supermarket Veruda",
        "Pula - Supermarket Stoja",
        "Pula - Supermarket Šijana",
        "Pula - Supermarket Vidikovac",
        "Karlovac - Supermarket Karlovačka 9",
        "Sisak - Supermarket Ul. Kralja Tomislava 56",
        "Varaždin - Supermarket Zagrebačka 12",
        "Šibenik - Supermarket Stjepana Radića 45",
        "Dubrovnik - Supermarket Lapadska obala 23",
        "Slavonski Brod - Supermarket Trg pobjede 7",
        "Bjelovar - Supermarket A. Mihanovića 15",
        "Koprivnica - Supermarket Hrvatske državnosti 8",
        "Čakovec - Supermarket Zrinsko Frankopanska 22",
        "Vinkovci - Supermarket Duga ulica 34",
        "Vukovar - Supermarket Vukovarska 56",
        "Samobor - Supermarket Trg kralja Tomislava 9",
        "Zaprešić - Supermarket Zagrebačka 45",
        "Opatija - Supermarket Maršala Tita 45",
        "Crikvenica - Supermarket Šetalište Vladimira Nazora 12",
        "Makarska - Supermarket Obala kralja Krešimira 9",
        "Trogir - Supermarket Gradska vrata 3",
        "Kaštela - Supermarket Kaštel Gomilica",
        "Solin - Supermarket Trg kralja Zvonimira 5",
    ]
    
    # Generate dates for last 3 days
    dates = [(datetime.now() - timedelta(days=i)).strftime("%d.%m.%Y") for i in range(3)]
    log(f"📅 Processing dates: {', '.join(dates)}")
    log(f"📍 Total locations: {len(locations)}")
    
    total_processed = 0
    files_processed = 0
    job["total"] = len(locations) * len(dates)  # Total possible files
    
    for idx, location in enumerate(locations, 1):
        log(f"📍 [{idx}/{len(locations)}] {location[:50]}...")
        
        for date_str in dates:
            # Generate filename
            if "Velika Gorica" in location:
                base_name = "SupermarketTrg-Grada-Vukovara-8-Velika-Gorica-10410"
                filename = f"{base_name}-{date_str}-7.00h-C30.csv"
            else:
                city = location.split(' - ')[0] if ' - ' in location else location
                clean_city = city.replace(' ', '-')
                clean_city = re.sub(r'[^\w-]', '', clean_city)
                filename = f"Supermarket-{clean_city}-{date_str}-7.00h-C30.csv"
            
            job["current_file"] = f"{location[:30]} - {date_str}"
            
            # Try to download
            current_month = datetime.now().strftime("%Y/%m")
            previous_month = (datetime.now() - timedelta(days=30)).strftime("%Y/%m")
            
            downloaded = False
            for month_path in [current_month, previous_month]:
                url = f"{base_url}/wp-content/uploads/{month_path}/{filename}"
                try:
                    r = requests.get(url, timeout=15)
                    if r.status_code == 200:
                        log(f"  ✓ [{date_str}] Downloaded: {filename}")
                        
                        # Parse CSV
                        try:
                            for encoding in ['utf-8', 'iso-8859-1', 'cp1250', 'latin1']:
                                try:
                                    df = pd.read_csv(
                                        io.BytesIO(r.content),
                                        encoding=encoding,
                                        sep=',',
                                        on_bad_lines='skip'
                                    )
                                    if not df.empty:
                                        break
                                except:
                                    continue
                            
                            if df is not None and not df.empty:
                                # Prepare records
                                records = []
                                for _, row in df.iterrows():
                                    if pd.notna(row.get('Barcode')) and pd.notna(row.get('MPC')):
                                        barcode = str(row.get('Barcode', '')).strip()
                                        if barcode and barcode != 'nan':
                                            records.append({
                                                "barcode": barcode,
                                                "name": str(row.get('Naziv artikla', ''))[:300],
                                                "brand": str(row.get('Marka', ''))[:200] if pd.notna(row.get('Marka')) else None,
                                                "category": str(row.get('Naziv grupe artikala', ''))[:200] if pd.notna(row.get('Naziv grupe artikala')) else None,
                                                "unit": str(row.get('Gramaža', ''))[:50] if pd.notna(row.get('Gramaža')) else None,
                                                "regular_price": float(row.get('MPC', 0)) if pd.notna(row.get('MPC')) else None,
                                                "current_price": float(row.get('MPC', 0)) if pd.notna(row.get('MPC')) else None,
                                            })
                                
                                if records:
                                    df_clean = pd.DataFrame(records)
                                    df_clean['store'] = store_type
                                    df_clean['location'] = location
                                    df_clean['is_on_sale'] = False
                                    df_clean['sale_price'] = None
                                    push_to_supabase(df_clean, store_type)
                                    total_processed += len(records)
                                    log(f"    ✓ Processed {len(records)} products")
                                    files_processed += 1
                                    job["processed"] = files_processed
                                
                                downloaded = True
                                break
                        except Exception as e:
                            log(f"    ✗ Parse error: {e}")
                except Exception as e:
                    continue
            
            if not downloaded:
                log(f"  ✗ [{date_str}] No file found")
            
            time.sleep(0.2)  # Small delay between requests
        
        # Update progress
        job["processed"] = files_processed
    
    log(f"✅ Zabac completed: {total_processed} products from {files_processed} files")
    
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

ALL_STORES = ["lidl", "tommy", "spar", "konzum", "kaufland", "plodine", "studenac", "zabac"]

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
        threading.Thread(target=run_job_with_log, kwargs={"stores": stores, "triggered_by": "manual"}, daemon=True).start()
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
    threading.Thread(target=run_job_with_log,
                 kwargs={"stores": [], "manual_files": filepaths, "manual_store": store, "triggered_by": "upload"},
                 daemon=True).start()
    return jsonify({"ok": True})

@app.route("/daily", methods=["GET", "POST"])
def daily():
    """
    Called by UptimeRobot 3x per day: 07:00, 10:00, 13:00 Croatian time.
    Smart: skips stores already successfully downloaded today.
    """
    secret = request.args.get("secret") or request.form.get("secret")
    if secret != UPLOAD_PASSWORD:
        return jsonify({"error": "Unauthorized"}), 403

    if job["running"]:
        return jsonify({"status": "already running", "store": job["store"]})

    today = str(date.today())
    stores_done = get_stores_done_today(today)
    stores_to_run = [s for s in ALL_STORES if s not in stores_done]

    if not stores_to_run:
        log(f"✅ All stores already updated for {today}")
        return jsonify({
            "status": "already_complete",
            "date": today,
            "stores_done": stores_done,
        })

    log(f"📅 Daily run {today}. Done: {stores_done}. Running: {stores_to_run}")
    threading.Thread(
        target=run_job_with_log,
        kwargs={"stores": stores_to_run, "triggered_by": "daily"},
        daemon=True
    ).start()

    return jsonify({
        "status": "started",
        "date": today,
        "stores_running": stores_to_run,
        "stores_already_done": stores_done,
    })

def get_stores_done_today(today_str):
    """Return list of stores that already have price data for today."""
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/store_prices",
            headers=db_headers(),
            params={
                "price_date": f"eq.{today_str}",
                "select": "store",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            rows = resp.json()
            return list(set(r["store"] for r in rows))
    except Exception as e:
        log(f"  ⚠️  Could not check done stores: {e}")
    return []


def run_job_with_log(stores, triggered_by="manual", manual_files=None, manual_store=None):
    """Wrapper around run_job that logs to schedule_log table."""
    start_time = datetime.now()
    log_id = None

    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/schedule_log",
            headers={**db_headers(), "Prefer": "return=representation"},
            json={
                "run_at": start_time.isoformat(),
                "stores": stores or ([manual_store] if manual_store else []),
                "status": "started",
                "notes": f"triggered_by={triggered_by}",
            },
            timeout=10,
        )
        if resp.status_code in (200, 201):
            rows = resp.json()
            if rows:
                log_id = rows[0].get("id")
    except Exception as e:
        log(f"  ⚠️  Could not write schedule_log start: {e}")

    # Run the actual job
    run_job(stores=stores or [], manual_files=manual_files, manual_store=manual_store)

    # Update log with result
    duration = int((datetime.now() - start_time).total_seconds())
    if log_id:
        try:
            requests.patch(
                f"{SUPABASE_URL}/rest/v1/schedule_log?id=eq.{log_id}",
                headers=db_headers(),
                json={
                    "status": "done" if not job["errors"] else "partial",
                    "products": job["processed"],
                    "errors": len(job["errors"]),
                    "duration_s": duration,
                },
                timeout=10,
            )
        except Exception as e:
            log(f"  ⚠️  Could not update schedule_log: {e}")


@app.route("/schedule/status")
def schedule_status():
    """Quick check: what's been downloaded today and what's missing."""
    today = str(date.today())
    done = get_stores_done_today(today)
    missing = [s for s in ALL_STORES if s not in done]
    return jsonify({
        "date": today,
        "done": done,
        "missing": missing,
        "all_complete": len(missing) == 0,
    })


@app.route("/schedule/history")
def schedule_history():
    """Show last 20 scheduled runs."""
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/schedule_log",
            headers=db_headers(),
            params={"order": "run_at.desc", "limit": "20"},
            timeout=10,
        )
        rows = resp.json() if resp.status_code == 200 else []
    except Exception:
        rows = []
    return jsonify(rows)

@app.route("/scan-zabac-ui")
def scan_zabac_ui():
    """HTML interface for Zabac scan"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Zabac Scanner</title>
        <style>
            body { font-family: monospace; padding: 20px; background: #111; color: #0f0; }
            pre { background: #222; padding: 15px; border-radius: 8px; overflow-x: auto; }
            .success { color: #0f0; }
            .failure { color: #f00; }
            .info { color: #ff0; }
        </style>
    </head>
    <body>
        <h1>🔍 Zabac File Scanner</h1>
        <div id="results">Loading...</div>
        
        <script>
            fetch('/scan-zabac?secret=' + prompt('Enter password:'))
                .then(r => r.json())
                .then(data => {
                    let html = '<h2>📊 Scan Results</h2><pre>';
                    data.scan_results.forEach(loc => {
                        html += `\\n📍 ${loc.location}\\n`;
                        for (const [date, exists] of Object.entries(loc.dates)) {
                            const status = exists ? '✅ EXISTS' : '❌ MISSING';
                            html += `  ${date}: ${status}\\n`;
                        }
                    });
                    html += `\\n📁 Files found: ${data.found_files.length}\\n`;
                    html += `\\n💡 ${data.conclusion.message}\\n`;
                    html += `📌 ${data.conclusion.recommendation}\\n`;
                    html += '</pre>';
                    document.getElementById('results').innerHTML = html;
                })
                .catch(err => {
                    document.getElementById('results').innerHTML = '<div class="failure">Error: ' + err + '</div>';
                });
        </script>
    </body>
    </html>
    """

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5001)), debug=False)
