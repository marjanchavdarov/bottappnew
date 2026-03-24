import os
import requests
import json
import time
from datetime import date
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
MCP_URL = "https://api.cijene.dev/mcp"

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

# Configuration
BATCH_SIZE_PRODUCTS = 500
BATCH_SIZE_PRICES = 200
PAGE_SIZE = 100
MAX_PAGES_PER_CHAIN = 100  # 10,000 products per chain max
DELAY_BETWEEN_CALLS = 0.5  # seconds
DELAY_BETWEEN_CHAINS = 2  # seconds

def call_mcp_tool(tool_name: str, arguments: Dict = None) -> List[Dict]:
    """Call MCP server tool with error handling and rate limiting"""
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
        # Rate limiting
        time.sleep(DELAY_BETWEEN_CALLS)
        
        response = requests.post(MCP_URL, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        if "error" in result:
            logger.error(f"MCP Error: {result['error']}")
            return []
        
        # Parse the response content
        content = result.get("result", {}).get("content", [])
        
        # Extract text from content
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
                    data.append({"text": item["text"]})
        
        return data
        
    except requests.exceptions.Timeout:
        logger.error(f"MCP timeout for {tool_name}")
        return []
    except Exception as e:
        logger.error(f"MCP call failed for {tool_name}: {e}")
        return []

def get_all_chains() -> List[Dict]:
    """Get list of all retail chains"""
    logger.info("Fetching chains from MCP...")
    chains = call_mcp_tool("list_chains")
    logger.info(f"Found {len(chains)} chains")
    return chains

def get_chain_stats(chain_name: str) -> Dict:
    """Get statistics for a chain"""
    stats = call_mcp_tool("get_stats", {"chain": chain_name})
    return stats[0] if stats else {}

def get_chain_stores(chain_name: str) -> List[Dict]:
    """Get all stores for a chain"""
    stores = call_mcp_tool("list_chain_stores", {"chain": chain_name})
    return stores

def search_products_by_chain(chain_name: str, limit: int = 100, offset: int = 0) -> List[Dict]:
    """Search products for a specific chain with pagination"""
    products = call_mcp_tool("search_products", {
        "query": "",
        "chain": chain_name,
        "limit": limit,
        "offset": offset
    })
    return products

def get_product_by_ean(ean: str) -> List[Dict]:
    """Get product details by EAN/barcode"""
    products = call_mcp_tool("get_products_by_ean", {"ean": ean})
    return products

def upsert_products(products: List[Dict]) -> int:
    """Upsert products to master_products table"""
    if not products or not supabase:
        return 0
    
    records = []
    for p in products:
        record = {
            "barcode": p.get("ean", ""),
            "name": p.get("name", ""),
            "brand": p.get("brand", ""),
            "category": p.get("category", "")
        }
        if record["barcode"]:
            records.append(record)
    
    if not records:
        return 0
    
    # Deduplicate by barcode
    unique_records = {r["barcode"]: r for r in records}.values()
    unique_list = list(unique_records)
    
    total = 0
    for i in range(0, len(unique_list), BATCH_SIZE_PRODUCTS):
        batch = unique_list[i:i+BATCH_SIZE_PRODUCTS]
        try:
            supabase.table("master_products").upsert(
                batch, 
                on_conflict="barcode"
            ).execute()
            total += len(batch)
            if total % 1000 == 0:
                logger.info(f"      Upserted {total} products")
        except Exception as e:
            logger.error(f"      Product upsert error: {e}")
    
    return total

def upsert_prices(prices: List[Dict]) -> int:
    """Upsert prices to store_prices table"""
    if not prices or not supabase:
        return 0
    
    records = []
    today = date.today().isoformat()
    
    for p in prices:
        if p.get("price") and p.get("barcode"):
            try:
                price_value = float(p.get("price", 0))
                if price_value > 0:
                    record = {
                        "barcode": p.get("barcode"),
                        "store": p.get("store", ""),
                        "price_date": today,
                        "current_price": price_value
                    }
                    records.append(record)
            except (ValueError, TypeError):
                continue
    
    if not records:
        return 0
    
    # Deduplicate
    unique_records = {}
    for r in records:
        key = f"{r['barcode']}_{r['store']}_{r['price_date']}"
        unique_records[key] = r
    
    unique_list = list(unique_records.values())
    
    total = 0
    for i in range(0, len(unique_list), BATCH_SIZE_PRICES):
        batch = unique_list[i:i+BATCH_SIZE_PRICES]
        try:
            supabase.table("store_prices").upsert(
                batch,
                on_conflict="barcode,store,price_date"
            ).execute()
            total += len(batch)
            
            if total % 1000 == 0:
                logger.info(f"      Upserted {total} prices")
                
        except Exception as e:
            logger.error(f"      Price upsert error: {e}")
            # If batch fails, try even smaller batches
            if len(batch) > 50:
                logger.info(f"      Retrying in smaller batches...")
                for sub_batch in [batch[j:j+50] for j in range(0, len(batch), 50)]:
                    try:
                        supabase.table("store_prices").upsert(
                            sub_batch,
                            on_conflict="barcode,store,price_date"
                        ).execute()
                        total += len(sub_batch)
                    except Exception as sub_e:
                        logger.error(f"      Sub-batch failed: {sub_e}")
    
    return total

def sync_chain_complete(chain_name: str):
    """Sync complete data for a single chain"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing chain: {chain_name}")
    logger.info(f"{'='*60}")
    
    try:
        # Get stats
        stats = get_chain_stats(chain_name)
        if stats:
            logger.info(f"Stats: {json.dumps(stats, indent=2)}")
        
        # Get stores
        stores = get_chain_stores(chain_name)
        logger.info(f"Found {len(stores)} stores")
        
        # Store store info in Supabase (optional)
        store_count = 0
        for store in stores:
            try:
                supabase.table("stores").upsert({
                    "store_id": store.get("id", store.get("name")),
                    "chain": chain_name,
                    "name": store.get("name"),
                    "city": store.get("city", ""),
                    "address": store.get("address", ""),
                    "lat": store.get("lat"),
                    "lng": store.get("lng")
                }, on_conflict="store_id").execute()
                store_count += 1
            except Exception as e:
                logger.debug(f"Store upsert error: {e}")
        
        logger.info(f"Upserted {store_count} stores")
        
        # Fetch all products with pagination
        all_products = []
        offset = 0
        
        logger.info(f"Fetching products (max {MAX_PAGES_PER_CHAIN * PAGE_SIZE} products)...")
        
        while offset < MAX_PAGES_PER_CHAIN * PAGE_SIZE:
            products = search_products_by_chain(chain_name, PAGE_SIZE, offset)
            
            if not products:
                break
            
            all_products.extend(products)
            logger.info(f"   Fetched {len(products)} products (total: {len(all_products)})")
            
            offset += PAGE_SIZE
            
            # Rate limiting
            time.sleep(DELAY_BETWEEN_CALLS)
        
        # Upsert products
        if all_products:
            logger.info(f"Upserting {len(all_products)} products...")
            product_count = upsert_products(all_products)
            logger.info(f"✅ Upserted {product_count} products")
            
            # Extract and upsert prices
            prices = []
            for p in all_products:
                if p.get("prices"):
                    for price_info in p["prices"]:
                        prices.append({
                            "barcode": p.get("ean"),
                            "store": f"{chain_name} - {price_info.get('store_name', '')}",
                            "price": price_info.get("price")
                        })
            
            if prices:
                logger.info(f"Upserting {len(prices)} prices...")
                price_count = upsert_prices(prices)
                logger.info(f"✅ Upserted {price_count} prices")
        
        logger.info(f"✅ Chain {chain_name} completed successfully")
        
    except Exception as e:
        logger.error(f"Error syncing {chain_name}: {e}")

def sync_specific_barcodes(barcodes: List[str]):
    """Sync specific products by barcode"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Syncing {len(barcodes)} specific barcodes...")
    logger.info(f"{'='*60}")
    
    all_products = []
    for barcode in barcodes:
        products = get_product_by_ean(barcode)
        if products:
            all_products.extend(products)
            logger.info(f"✅ Found product for {barcode}")
        else:
            logger.warning(f"⚠️ No product found for {barcode}")
        
        time.sleep(DELAY_BETWEEN_CALLS)
    
    if all_products:
        logger.info(f"Upserting {len(all_products)} products...")
        upsert_products(all_products)
        
        # Extract prices
        prices = []
        for p in all_products:
            if p.get("prices"):
                for price_info in p["prices"]:
                    prices.append({
                        "barcode": p.get("ean"),
                        "store": f"{price_info.get('chain', '')} - {price_info.get('store_name', '')}",
                        "price": price_info.get("price")
                    })
        
        if prices:
            logger.info(f"Upserting {len(prices)} prices...")
            upsert_prices(prices)

def main():
    """Main function to run the sync"""
    logger.info("="*60)
    logger.info("🌍 Starting MCP-based data sync")
    logger.info("="*60)
    start_time = time.time()
    
    # Check credentials
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("❌ Missing Supabase credentials")
        logger.error("Please set SUPABASE_URL and SUPABASE_KEY in environment variables")
        return
    
    if not supabase:
        logger.error("❌ Failed to initialize Supabase client")
        return
    
    logger.info(f"✅ Supabase URL: {SUPABASE_URL}")
    logger.info(f"✅ MCP URL: {MCP_URL}")
    
    # Get all chains
    chains = get_all_chains()
    
    if not chains:
        logger.error("❌ No chains found from MCP")
        return
    
    logger.info(f"\n📊 Found {len(chains)} chains to process:")
    for chain in chains:
        chain_name = chain.get("name", chain.get("id", "Unknown"))
        logger.info(f"   - {chain_name}")
    
    # Sync all chains
    logger.info(f"\n🔄 Syncing all chains...")
    
    for chain in chains:
        chain_name = chain.get("name", chain.get("id"))
        if chain_name:
            sync_chain_complete(chain_name)
        
        # Add delay between chains
        time.sleep(DELAY_BETWEEN_CHAINS)
    
    elapsed = time.time() - start_time
    logger.info("\n" + "="*60)
    logger.info(f"🏁 SYNC COMPLETE! (took {elapsed/60:.1f} minutes)")
    logger.info("="*60)

if __name__ == "__main__":
    main()
