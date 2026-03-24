import os
import requests
import json
import time
from datetime import date
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
MCP_URL = "https://api.cijene.dev/mcp"

print("=" * 60)
print("🌍 MCP Price Sync Started")
print("=" * 60)

# Check credentials
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Missing Supabase credentials")
    exit(1)

print(f"✅ Supabase URL: {SUPABASE_URL}")

# Initialize Supabase
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Supabase client initialized")
except Exception as e:
    print(f"❌ Failed to initialize Supabase: {e}")
    exit(1)

def call_mcp_api(endpoint, data=None):
    """Call MCP API with proper format"""
    try:
        if data:
            response = requests.post(f"{MCP_URL}/{endpoint}", json=data, timeout=30)
        else:
            response = requests.get(f"{MCP_URL}/{endpoint}", timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"   ⚠️ API returned {response.status_code}")
            return None
    except Exception as e:
        print(f"   ❌ API call failed: {e}")
        return None

def get_chains():
    """Get list of chains from the API"""
    print("📡 Fetching chains...")
    
    # Try different endpoints
    endpoints = ["/chains", "/list", "/v0/chains", "/api/chains"]
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"https://api.cijene.dev{endpoint}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Found chains via {endpoint}")
                return data
        except:
            continue
    
    # Fallback: known chains from Croatia
    print("⚠️ Using fallback chain list")
    return [
        {"id": "konzum", "name": "KONZUM"},
        {"id": "lidle", "name": "LIDL"},
        {"id": "spar", "name": "SPAR"},
        {"id": "plodine", "name": "PLODINE"},
        {"id": "studenac", "name": "STUDENAC"},
        {"id": "dm", "name": "DM"},
        {"id": "boso", "name": "BOSO"},
        {"id": "eurospin", "name": "EUROSPIN"}
    ]

def get_products_by_chain(chain_name, limit=100):
    """Get products for a specific chain"""
    # Try different API formats
    endpoints = [
        f"/v0/products?chain={chain_name}&limit={limit}",
        f"/products?chain={chain_name}&limit={limit}",
        f"/api/products?chain={chain_name}&limit={limit}"
    ]
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"https://api.cijene.dev{endpoint}", timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data:
                    return data
        except:
            continue
    
    # If API fails, return empty
    print(f"   ⚠️ No products found for {chain_name}")
    return []

def upsert_products(products):
    """Upsert products to Supabase"""
    if not products:
        return 0
    
    records = []
    for p in products:
        ean = p.get("ean") or p.get("barcode") or p.get("gtin")
        name = p.get("name") or p.get("product_name")
        
        if ean and name:
            records.append({
                "barcode": str(ean),
                "name": name,
                "brand": p.get("brand", ""),
                "category": p.get("category", "")
            })
    
    if not records:
        return 0
    
    # Remove duplicates
    unique = {r["barcode"]: r for r in records}
    unique_list = list(unique.values())
    
    # Upsert in batches
    batch_size = 500
    total = 0
    
    for i in range(0, len(unique_list), batch_size):
        batch = unique_list[i:i+batch_size]
        try:
            supabase.table("master_products").upsert(batch).execute()
            total += len(batch)
            if total % 1000 == 0:
                print(f"      📦 Upserted {total} products")
        except Exception as e:
            print(f"      ❌ Product upsert error: {e}")
    
    return total

def upsert_prices(products, chain_name):
    """Extract and upsert prices from products"""
    if not products:
        return 0
    
    today = date.today().isoformat()
    records = []
    
    for p in products:
        ean = p.get("ean") or p.get("barcode") or p.get("gtin")
        
        if ean:
            # Try to find price data
            price = p.get("price") or p.get("current_price")
            store = p.get("store") or p.get("store_name") or chain_name
            
            if price:
                try:
                    price_float = float(price)
                    if price_float > 0:
                        records.append({
                            "barcode": str(ean),
                            "store": f"{chain_name} - {store}",
                            "price_date": today,
                            "current_price": price_float
                        })
                except (ValueError, TypeError):
                    pass
    
    if not records:
        return 0
    
    # Remove duplicates
    unique = {}
    for r in records:
        key = f"{r['barcode']}_{r['store']}_{r['price_date']}"
        unique[key] = r
    unique_list = list(unique.values())
    
    # Upsert in small batches
    batch_size = 200
    total = 0
    
    for i in range(0, len(unique_list), batch_size):
        batch = unique_list[i:i+batch_size]
        try:
            supabase.table("store_prices").upsert(batch).execute()
            total += len(batch)
            if total % 1000 == 0:
                print(f"      💰 Upserted {total} prices")
        except Exception as e:
            print(f"      ❌ Price upsert error: {e}")
    
    return total

def main():
    """Main sync function"""
    print("\n" + "="*60)
    print("Starting MCP-based sync...")
    print("="*60)
    
    # Get chains
    chains = get_chains()
    
    if not chains:
        print("❌ No chains found")
        return
    
    print(f"✅ Found {len(chains)} chains to process")
    
    # Process each chain
    for idx, chain in enumerate(chains, 1):
        chain_name = chain.get("name", chain.get("id", "Unknown")).upper()
        
        print(f"\n{'='*50}")
        print(f"[{idx}/{len(chains)}] 🏪 Processing: {chain_name}")
        print(f"{'='*50}")
        
        try:
            # Get products for this chain
            products = get_products_by_chain(chain_name, limit=200)  # Start with 200 products
            
            if not products:
                print(f"   ⚠️ No products found for {chain_name}")
                continue
            
            print(f"   📦 Found {len(products)} products")
            
            # Upsert products
            product_count = upsert_products(products)
            print(f"   ✅ Upserted {product_count} products")
            
            # Upsert prices
            price_count = upsert_prices(products, chain_name)
            if price_count > 0:
                print(f"   ✅ Upserted {price_count} prices")
            else:
                print(f"   ⚠️ No prices found")
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            print(f"   ❌ Error processing {chain_name}: {e}")
            continue
    
    print("\n" + "="*60)
    print("🏁 SYNC COMPLETE!")
    print("="*60)

if __name__ == "__main__":
    main()
