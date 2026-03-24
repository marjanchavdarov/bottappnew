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
    print("Make sure SUPABASE_URL and SUPABASE_KEY are set in GitHub Secrets")
    exit(1)

print(f"✅ Supabase URL: {SUPABASE_URL}")
print(f"✅ MCP URL: {MCP_URL}")

# Initialize Supabase
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Supabase client initialized")
except Exception as e:
    print(f"❌ Failed to initialize Supabase: {e}")
    exit(1)

def call_mcp_tool(tool_name, arguments=None):
    """Call MCP server tool"""
    if arguments is None:
        arguments = {}
    
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        },
        "id": 1
    }
    
    try:
        time.sleep(0.5)  # Rate limiting
        response = requests.post(MCP_URL, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"   ⚠️ MCP returned {response.status_code}")
            return []
        
        result = response.json()
        
        if "error" in result:
            print(f"   ❌ MCP Error: {result['error']}")
            return []
        
        # Parse content
        content = result.get("result", {}).get("content", [])
        data = []
        
        for item in content:
            if item.get("type") == "text":
                try:
                    parsed = json.loads(item["text"])
                    if isinstance(parsed, list):
                        data.extend(parsed)
                    else:
                        data.append(parsed)
                except json.JSONDecodeError:
                    pass
        
        return data
        
    except requests.exceptions.Timeout:
        print(f"   ⏰ Timeout calling {tool_name}")
        return []
    except Exception as e:
        print(f"   ❌ Error calling {tool_name}: {e}")
        return []

def upsert_products(products):
    """Upsert products to Supabase"""
    if not products:
        return 0
    
    records = []
    for p in products:
        ean = p.get("ean", "")
        if ean:
            records.append({
                "barcode": ean,
                "name": p.get("name", ""),
                "brand": p.get("brand", ""),
                "category": p.get("category", "")
            })
    
    if not records:
        return 0
    
    # Remove duplicates by barcode
    unique = {}
    for r in records:
        unique[r["barcode"]] = r
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

def upsert_prices(prices):
    """Upsert prices to Supabase"""
    if not prices:
        return 0
    
    today = date.today().isoformat()
    records = []
    
    for p in prices:
        barcode = p.get("barcode")
        price_val = p.get("price")
        store = p.get("store")
        
        if barcode and price_val and store:
            try:
                price_float = float(price_val)
                if price_float > 0:
                    records.append({
                        "barcode": barcode,
                        "store": store,
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
            # Try smaller batches if failed
            if len(batch) > 50:
                for sub_batch in [batch[j:j+50] for j in range(0, len(batch), 50)]:
                    try:
                        supabase.table("store_prices").upsert(sub_batch).execute()
                        total += len(sub_batch)
                    except:
                        pass
    
    return total

def main():
    print("\n📡 Fetching chains from MCP...")
    chains = call_mcp_tool("list_chains")
    
    if not chains:
        print("❌ No chains found")
        return
    
    print(f"✅ Found {len(chains)} chains")
    
    # Process each chain
    for idx, chain in enumerate(chains, 1):
        chain_name = chain.get("name", chain.get("id", "Unknown"))
        print(f"\n{'='*50}")
        print(f"[{idx}/{len(chains)}] 🏪 Processing: {chain_name}")
        print(f"{'='*50}")
        
        try:
            # Fetch products (limit to 500 for testing, remove limit for full sync)
            print(f"   🔍 Fetching products...")
            products = call_mcp_tool("search_products", {
                "query": "",
                "chain": chain_name,
                "limit": 500  # Increase this for full sync
            })
            
            if not products:
                print(f"   ⚠️ No products found for {chain_name}")
                continue
            
            print(f"   📦 Found {len(products)} products")
            
            # Upsert products
            product_count = upsert_products(products)
            print(f"   ✅ Upserted {product_count} products")
            
            # Extract prices
            prices = []
            for p in products:
                ean = p.get("ean")
                if ean and p.get("prices"):
                    for price_info in p["prices"]:
                        prices.append({
                            "barcode": ean,
                            "store": f"{chain_name} - {price_info.get('store_name', '')}",
                            "price": price_info.get("price")
                        })
            
            if prices:
                print(f"   💰 Found {len(prices)} prices")
                price_count = upsert_prices(prices)
                print(f"   ✅ Upserted {price_count} prices")
            else:
                print(f"   ⚠️ No prices found")
            
            # Rate limiting between chains
            time.sleep(2)
            
        except Exception as e:
            print(f"   ❌ Error processing {chain_name}: {e}")
            continue
    
    print("\n" + "=" * 60)
    print("🏁 MCP SYNC COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    main()
