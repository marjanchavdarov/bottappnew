"""
test_connection.py — verify your Supabase setup is correct before running ingest.py

Usage:
    python test_connection.py
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

REQUIRED_TABLES = ["master_products", "store_prices"]

def headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
    }

def check(label, passed, detail=""):
    icon = "✅" if passed else "❌"
    print(f"  {icon}  {label}")
    if detail:
        print(f"       {detail}")
    return passed

def main():
    all_ok = True
    print("\n🔍 katalog-prices — Supabase connection test\n")

    # 1. Env vars present
    ok = check(
        "SUPABASE_URL set",
        bool(SUPABASE_URL),
        SUPABASE_URL if SUPABASE_URL else "Missing in .env"
    )
    all_ok = all_ok and ok

    ok = check(
        "SUPABASE_KEY set",
        bool(SUPABASE_KEY),
        f"{SUPABASE_KEY[:12]}..." if SUPABASE_KEY else "Missing in .env"
    )
    all_ok = all_ok and ok

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("\n❌ Fix your .env file first (copy .env.example → .env and fill in values)\n")
        return

    # 2. Can we reach Supabase at all
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/",
            headers=headers(),
            timeout=10
        )
        ok = check(
            "Supabase reachable",
            resp.status_code in (200, 400),   # 400 = reached but no table specified, still OK
            f"HTTP {resp.status_code}"
        )
        all_ok = all_ok and ok
    except Exception as e:
        check("Supabase reachable", False, str(e))
        all_ok = False

    # 3. Check each required table exists and is readable
    print()
    for table in REQUIRED_TABLES:
        try:
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/{table}?limit=1",
                headers=headers(),
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                ok = check(
                    f"Table '{table}' exists and readable",
                    True,
                    f"{len(data)} row(s) found" if data else "Empty (that's fine, you haven't ingested yet)"
                )
            elif resp.status_code == 404:
                ok = check(
                    f"Table '{table}' exists",
                    False,
                    "Table not found — did you run create_tables.sql in Supabase SQL Editor?"
                )
            else:
                ok = check(
                    f"Table '{table}' accessible",
                    False,
                    f"HTTP {resp.status_code}: {resp.text[:200]}"
                )
            all_ok = all_ok and ok
        except Exception as e:
            check(f"Table '{table}'", False, str(e))
            all_ok = False

    # 4. Quick write test — insert + delete a dummy row
    print()
    try:
        test_row = {
            "barcode":  "0000000000000",
            "name":     "TEST PRODUCT — safe to delete",
            "category": "test",
        }
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/master_products",
            headers={**headers(), "Prefer": "return=representation"},
            json=test_row,
            timeout=10
        )
        write_ok = resp.status_code in (200, 201)
        check("Write to master_products", write_ok, f"HTTP {resp.status_code}")
        all_ok = all_ok and write_ok

        if write_ok:
            # Clean up test row
            del_resp = requests.delete(
                f"{SUPABASE_URL}/rest/v1/master_products?barcode=eq.0000000000000",
                headers=headers(),
                timeout=10
            )
            check("Cleanup test row", del_resp.status_code in (200, 204), "")

    except Exception as e:
        check("Write test", False, str(e))
        all_ok = False

    # 5. Summary
    print()
    if all_ok:
        print("✅ Everything looks good! You can now run:")
        print("   python ingest.py --store konzum --file your_file.csv --preview-only\n")
    else:
        print("❌ Some checks failed. Fix the issues above before running ingest.py\n")

if __name__ == "__main__":
    main()
