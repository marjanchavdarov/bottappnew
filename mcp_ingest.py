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
BASE_URL = "https://api.cijene.dev"

print("=" * 60)
print("🌍 Cijene API Sync Started")
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

def get_chains():
    """Get list of chains from the API"""
    print("📡 Fetching chains...")
    try:
        # Try to get chains from the API
        response = requests.get(f"{BASE_URL}/v0/chains", timeout=10)
        if response.status_code == 200:
            chains = response.json()
            print(f"✅ Found {len(chains)} chains from API")
            return chains
    except:
        pass
    
    # Fallback chains (Croatian retailers)
    print("⚠️ Using fallback chain list")
    return [
        {"id": "konzum", "name": "Konzum"},
        {"id": "lidl", "name": "Lidl"},
        {"id": "spar", "name": "Spar"},
        {"id": "plodine", "name": "Plodine"},
        {"id": "studenac", "name": "Studenac"},
        {"id": "dm", "name": "DM"},
        {"id": "boso", "name": "Boso"},
        {"id": "eurospin", "name": "Eurospin"},
        {"id": "tommy", "name": "Tommy"},
        {"id": "ktc", "name": "KTC"},
        {"id": "metro", "name": "Metro"},
        {"id": "kaufland", "name": "Kaufland"}
    ]

def get_products_by_chain(chain_name, limit=500, offset=0):
    """Get products for a specific chain from the API"""
    try:
        # Try different API endpoints
        endpoints = [
            f"{BASE_URL}/v0/products?chain={chain_name}&limit={limit}&offset={offset}",
            f"{BASE_URL}/v0/search?chain={chain_name}&limit={limit}&offset={offset}",
            f"{BASE_URL}/api/products?chain={chain_name}&limit={limit}"
        ]
        
        for endpoint in endpoints:
            try:
                response = requests.get(endpoint, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        return data
            except:
                continue
        
        # If all fail, try archive approach
        return get_products_from_archive(chain_name, limit)
        
    except Exception as e:
        print(f"   ❌ Error fetching products: {e}")
        return []

def get_products_from_archive(chain_name, limit=500):
    """Fallback: get products from archive data"""
    try:
        # Get latest archive info
        response = requests.get(f"{BASE_URL}/v0/list", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("archives"):
                archive_url = data["archives"][0]["url"]
                print(f"   📦 Using archive: {archive_url}")
                
                # Download and parse archive (simplified)
                # For now, return empty to avoid complexity
                return []
    except:
        pass
    
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
    print("Starting Cijene API sync...")
    print("="*60)
    
    # Get chains
    chains = get_chains()
    
    if not chains:
        print("❌ No chains found")
        return
    
    print(f"✅ Found {len(chains)} chains to process")
    
    # Process each chain
    total_products = 0
    total_prices = 0
    
    for idx, chain in enumerate(chains, 1):
        chain_name = chain.get("name", chain.get("id", "Unknown"))
        
        print(f"\n{'='*50}")
        print(f"[{idx}/{len(chains)}] 🏪 Processing: {chain_name}")
        print(f"{'='*50}")
        
        try:
            # Get products for this chain
            products = get_products_by_chain(chain_name, limit=500)
            
            if not products:
                print(f"   ⚠️ No products found for {chain_name}")
                continue
            
            print(f"   📦 Found {len(products)} products")
            
            # Upsert products
            product_count = upsert_products(products)
            total_products += product_count
            print(f"   ✅ Upserted {product_count} products")
            
            # Upsert prices
            price_count = upsert_prices(products, chain_name)
            total_prices += price_count
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
    print(f"🏁 SYNC COMPLETE!")
    print(f"   Total products upserted: {total_products}")
    print(f"   Total prices upserted: {total_prices}")
    print("="*60)

if __name__ == "__main__":
    main()
