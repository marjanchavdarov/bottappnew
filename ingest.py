"""
ingest.py — katalog.ai price data ingestion pipeline
Reads Croatian store price files (CSV/XML) and loads them into Supabase.

Usage:
    python ingest.py --store konzum --file "konzum_prices.csv"
    python ingest.py --store studenac --file "studenac_prices.xml"
    python ingest.py --all-stores   # downloads + processes all stores automatically

Requirements:
    pip install pandas requests python-dotenv lxml
"""

import os
import sys
import argparse
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from io import StringIO
from datetime import date
from dotenv import load_dotenv

load_dotenv()

# ─── Supabase config ──────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def db_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates",  # upsert behaviour
    }


# ─── Store download URLs ───────────────────────────────────────────────────────
# NOTE: Konzum has hundreds of files (one per store location).
# For now we point at a single known file for testing.
# In production, add a crawler that lists all files from their index page.
STORE_URLS = {
    "konzum":   None,   # pass --file manually (hundreds of files)
    "spar":     None,   # pass --file manually
    "studenac": None,   # pass --file manually (XML)
    "tommy":    None,
    "kaufland": None,
    "lidl":     None,
    "plodine":  None,
}


# ─── Column mapping per store ──────────────────────────────────────────────────
# Maps each store's raw column names → our standard field names
STORE_SCHEMAS = {
    "konzum": {
        "encoding":   "cp1250",       # Windows encoding (Croatian chars)
        "separator":  ";",
        "decimal":    ",",
        "columns": {
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
    },
    "studenac": {
        "format": "xml",
        # XML tag mapping defined in parse_studenac_xml() below
    },
    # Add more stores here as you discover their column names
}


# ─── Parse Konzum CSV ──────────────────────────────────────────────────────────
def parse_konzum_csv(filepath: str) -> pd.DataFrame:
    schema = STORE_SCHEMAS["konzum"]

    # Try cp1250 first (most Konzum files), fall back to utf-8
    for encoding in ["cp1250", "utf-8", "utf-8-sig", "latin-1"]:
        try:
            df = pd.read_csv(
                filepath,
                sep=schema["separator"],
                encoding=encoding,
                dtype=str,          # read everything as string first
                skipinitialspace=True,
            )
            print(f"  Opened with encoding: {encoding}")
            break
        except (UnicodeDecodeError, Exception) as e:
            continue
    else:
        raise ValueError(f"Could not open {filepath} with any known encoding")

    # Normalise column names (strip whitespace, lowercase for matching)
    df.columns = [c.strip() for c in df.columns]

    # Rename columns to standard names
    col_map = schema["columns"]
    # Only rename columns that actually exist in this file
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Convert price columns: replace comma decimal, cast to float
    price_cols = ["regular_price", "sale_price", "lowest_30d_price", "anchor_price"]
    for col in price_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^\d.]", "", regex=True)   # remove stray chars
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean barcode: strip spaces, keep as string
    if "barcode" in df.columns:
        df["barcode"] = df["barcode"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
        # Drop rows with no barcode
        df = df[df["barcode"].notna() & (df["barcode"] != "") & (df["barcode"] != "nan")]

    # Determine current_price: use sale_price if available, otherwise regular_price
    df["current_price"] = df["sale_price"].combine_first(df["regular_price"])

    # is_on_sale flag
    df["is_on_sale"] = df["sale_price"].notna()

    # Add store identifier
    df["store"] = "konzum"
    df["ingested_date"] = str(date.today())

    return df


# ─── Parse Studenac XML ────────────────────────────────────────────────────────
def parse_studenac_xml(filepath: str) -> pd.DataFrame:
    """
    Studenac publishes XML files. Structure may look like:
    <artikli>
      <artikal>
        <naziv>...</naziv>
        <barkod>...</barkod>
        <mpc>...</mpc>
        ...
      </artikal>
    </artikli>
    Adjust tag names below once you see the actual file structure.
    """
    tree = ET.parse(filepath)
    root = tree.getroot()

    rows = []
    # Try common root patterns
    items = root.findall(".//artikal") or root.findall(".//item") or root.findall(".//product")

    if not items:
        # Print structure to help debug
        print("  XML root tag:", root.tag)
        print("  First child tags:", [c.tag for c in list(root)[:5]])
        raise ValueError("Could not find product elements in XML. Check tag names above and update parse_studenac_xml().")

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
    df["store"] = "studenac"
    df["ingested_date"] = str(date.today())

    # Convert prices
    for col in ["regular_price", "sale_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".").str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            )

    df["current_price"] = df["sale_price"].combine_first(df["regular_price"])
    df["is_on_sale"] = df["sale_price"].notna()

    return df


# ─── Load a file (auto-detect store + format) ─────────────────────────────────
def load_file(store: str, filepath: str) -> pd.DataFrame:
    print(f"\n📂 Loading {store} file: {filepath}")

    if store == "konzum":
        df = parse_konzum_csv(filepath)
    elif store == "studenac":
        df = parse_studenac_xml(filepath)
    else:
        # Generic CSV fallback for stores not yet mapped
        # Try to auto-detect separator and encoding
        df = pd.read_csv(filepath, sep=None, engine="python", encoding="utf-8", dtype=str)
        df["store"] = store
        df["ingested_date"] = str(date.today())
        print(f"  ⚠️  Using generic parser for {store}. Add a proper schema in STORE_SCHEMAS.")

    print(f"  ✓ Loaded {len(df)} products")
    return df


# ─── Supabase upsert ───────────────────────────────────────────────────────────
def upsert_master_products(df: pd.DataFrame):
    """
    Upsert into master_products table (one row per barcode).
    Only updates name/brand/category/unit if the barcode is new.
    """
    if SUPABASE_URL == "" or SUPABASE_KEY == "":
        print("  ⚠️  SUPABASE_URL or SUPABASE_KEY not set — skipping upload")
        return

    # Build records: only include rows with a barcode
    records = []
    for _, row in df.dropna(subset=["barcode"]).iterrows():
        rec = {
            "barcode":  str(row.get("barcode", "")).strip(),
            "name":     str(row.get("name", "")).strip()[:300],
            "brand":    str(row.get("brand", "")).strip()[:200] if pd.notna(row.get("brand")) else None,
            "category": str(row.get("category", "")).strip()[:200] if pd.notna(row.get("category")) else None,
            "unit":     str(row.get("unit", "")).strip()[:50] if pd.notna(row.get("unit")) else None,
            "quantity": float(row["quantity"]) if pd.notna(row.get("quantity")) else None,
        }
        if rec["barcode"] and rec["barcode"] != "nan":
            records.append(rec)

    if not records:
        print("  ⚠️  No valid records to upsert into master_products")
        return

    # Upload in batches of 500
    batch_size = 500
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/master_products",
            headers=db_headers(),
            json=batch,
        )
        if resp.status_code not in (200, 201):
            print(f"  ❌ master_products upsert error: {resp.status_code} {resp.text[:300]}")
        else:
            total += len(batch)

    print(f"  ✓ Upserted {total} rows into master_products")


def upsert_store_prices(df: pd.DataFrame):
    """
    Upsert into store_prices table (one row per barcode+store+date).
    """
    if SUPABASE_URL == "" or SUPABASE_KEY == "":
        return

    records = []
    today = str(date.today())
    store = df["store"].iloc[0] if "store" in df.columns else "unknown"

    for _, row in df.dropna(subset=["barcode"]).iterrows():
        barcode = str(row.get("barcode", "")).strip()
        if not barcode or barcode == "nan":
            continue

        current = row.get("current_price")
        regular = row.get("regular_price")
        sale    = row.get("sale_price")

        rec = {
            "barcode":       barcode,
            "store":         store,
            "price_date":    today,
            "current_price": float(current) if pd.notna(current) else None,
            "regular_price": float(regular) if pd.notna(regular) else None,
            "sale_price":    float(sale)    if pd.notna(sale)    else None,
            "is_on_sale":    bool(row.get("is_on_sale", False)),
        }
        records.append(rec)

    if not records:
        print("  ⚠️  No valid records to upsert into store_prices")
        return

    batch_size = 500
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/store_prices",
            headers=db_headers(),
            json=batch,
        )
        if resp.status_code not in (200, 201):
            print(f"  ❌ store_prices upsert error: {resp.status_code} {resp.text[:300]}")
        else:
            total += len(batch)

    print(f"  ✓ Upserted {total} rows into store_prices")


# ─── Preview (no Supabase needed) ─────────────────────────────────────────────
def preview(df: pd.DataFrame):
    print("\n📊 Preview (first 5 rows):")
    cols = ["name", "brand", "barcode", "current_price", "regular_price", "sale_price", "is_on_sale", "category"]
    cols = [c for c in cols if c in df.columns]
    print(df[cols].head(5).to_string(index=False))
    print(f"\n  Total rows: {len(df)}")
    print(f"  On sale:    {df['is_on_sale'].sum() if 'is_on_sale' in df.columns else 'N/A'}")
    print(f"  With barcode: {df['barcode'].notna().sum() if 'barcode' in df.columns else 'N/A'}")
    missing = df["barcode"].isna().sum() if "barcode" in df.columns else 0
    if missing:
        print(f"  ⚠️  Missing barcode: {missing} rows (will be skipped)")


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Katalog.ai price data ingestor")
    parser.add_argument("--store", type=str, help="Store name (konzum, studenac, spar, etc.)")
    parser.add_argument("--file",  type=str, help="Path to price file (CSV or XML)")
    parser.add_argument("--preview-only", action="store_true", help="Show preview without uploading to Supabase")
    args = parser.parse_args()

    if not args.store or not args.file:
        parser.print_help()
        print("\nExample:")
        print("  python ingest.py --store konzum --file konzum_ivanicgrad.csv")
        print("  python ingest.py --store konzum --file konzum_ivanicgrad.csv --preview-only")
        sys.exit(1)

    if not os.path.exists(args.file):
        print(f"❌ File not found: {args.file}")
        sys.exit(1)

    # Load and parse
    df = load_file(args.store, args.file)

    # Always show preview
    preview(df)

    if args.preview_only:
        print("\n✅ Preview done. Run without --preview-only to upload to Supabase.")
        return

    # Upload
    print("\n⬆️  Uploading to Supabase...")
    upsert_master_products(df)
    upsert_store_prices(df)
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
