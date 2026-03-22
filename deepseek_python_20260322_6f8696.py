import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json

def deep_scan_zabac():
    """Deep scan of Zabac website to discover all available price files"""
    base_url = "https://zabacfoodoutlet.hr"
    
    print("="*80)
    print("🔍 ZABAC DEEP SCAN")
    print("="*80)
    
    discovered_data = {
        "locations": [],
        "csv_files": [],
        "upload_directories": [],
        "api_endpoints": []
    }
    
    # 1. Check main cjenik page
    print("\n📄 1. Scanning main cjenik page...")
    try:
        r = requests.get(f"{base_url}/cjenik/", timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find all location options
        select = soup.find('select', {'name': 'lokacija'})
        if select:
            options = select.find_all('option')
            for opt in options:
                if opt.text.strip() and opt.text.strip() != 'Odaberi lokaciju':
                    discovered_data["locations"].append({
                        "name": opt.text.strip(),
                        "value": opt.get('value', '')
                    })
            print(f"  ✓ Found {len(discovered_data['locations'])} locations in dropdown")
        
        # Look for any CSV links in the page
        csv_links = re.findall(r'href="([^"]+\.csv)"', r.text)
        for link in csv_links:
            if not link.startswith('http'):
                link = base_url + link
            discovered_data["csv_files"].append(link)
        print(f"  ✓ Found {len(csv_links)} direct CSV links")
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # 2. Try to find the AJAX endpoint that provides locations
    print("\n🌐 2. Looking for AJAX endpoints...")
    ajax_url = f"{base_url}/wp-admin/admin-ajax.php"
    test_actions = ['get_locations', 'get_cjenik_locations', 'load_locations', 'fetch_locations']
    
    for action in test_actions:
        try:
            r = requests.post(ajax_url, data={'action': action}, timeout=10)
            if r.status_code == 200:
                try:
                    data = r.json()
                    print(f"  ✓ Found endpoint: {action} -> {data}")
                    discovered_data["api_endpoints"].append({action: data})
                except:
                    if r.text:
                        print(f"  ✓ Found endpoint: {action} -> {r.text[:200]}")
        except:
            pass
    
    # 3. Scan sitemaps for CSV files
    print("\n🗺️ 3. Scanning sitemaps...")
    sitemap_urls = [
        f"{base_url}/wp-sitemap.xml",
        f"{base_url}/sitemap.xml",
        f"{base_url}/wp-sitemap-posts-attachment-1.xml",
        f"{base_url}/wp-sitemap-posts-attachment-2.xml",
        f"{base_url}/wp-sitemap-posts-attachment-3.xml",
    ]
    
    for sitemap_url in sitemap_urls:
        try:
            r = requests.get(sitemap_url, timeout=10)
            if r.status_code == 200:
                if 'xml' in r.headers.get('content-type', ''):
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(r.content)
                    for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
                        url = loc.text
                        if '.csv' in url:
                            discovered_data["csv_files"].append(url)
                            print(f"  ✓ Found CSV: {url.split('/')[-1]}")
        except:
            pass
    
    # 4. Try to brute-force find upload directories
    print("\n📁 4. Checking upload directories...")
    months_to_check = [
        f"2026/{m:02d}" for m in range(1, 4)  # Jan-Mar 2026
    ] + [
        f"2025/{m:02d}" for m in range(1, 13)  # All 2025
    ]
    
    for month in months_to_check:
        dir_url = f"{base_url}/wp-content/uploads/{month}/"
        try:
            r = requests.get(dir_url, timeout=5)
            if r.status_code == 200:
                discovered_data["upload_directories"].append(month)
                # Try to find CSV files in directory listing
                csv_in_dir = re.findall(r'href="([^"]+\.csv)"', r.text)
                for csv_file in csv_in_dir:
                    discovered_data["csv_files"].append(f"{dir_url}{csv_file}")
                if csv_in_dir:
                    print(f"  ✓ {month}: Found {len(csv_in_dir)} CSV files")
                else:
                    print(f"  ✓ {month}: Directory exists")
        except:
            pass
    
    # 5. Check robots.txt for hints
    print("\n🤖 5. Checking robots.txt...")
    try:
        r = requests.get(f"{base_url}/robots.txt", timeout=10)
        if r.status_code == 200:
            print(f"  Found robots.txt")
            # Look for sitemap references
            sitemaps = re.findall(r'Sitemap:\s*(.+)', r.text, re.IGNORECASE)
            for sitemap in sitemaps:
                print(f"  Sitemap reference: {sitemap}")
    except:
        pass
    
    # 6. Try to find the actual download links from the location dropdown
    print("\n🔄 6. Simulating location selection...")
    # First, get the nonce/security token
    try:
        r = requests.get(f"{base_url}/cjenik/", timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find nonce in the page
        nonce = None
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'nonce' in script.string:
                match = re.search(r'nonce["\']?\s*:\s*["\']([^"\']+)["\']', script.string)
                if match:
                    nonce = match.group(1)
                    print(f"  ✓ Found nonce: {nonce}")
                    break
        
        # Try to get locations via AJAX
        if nonce:
            ajax_data = {
                'action': 'get_locations',
                'security': nonce,
                'post_id': 1
            }
            r = requests.post(ajax_url, data=ajax_data, timeout=10)
            if r.status_code == 200:
                try:
                    data = r.json()
                    print(f"  ✓ AJAX response: {json.dumps(data, indent=2)[:500]}")
                except:
                    print(f"  ✓ AJAX response: {r.text[:500]}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # 7. Try common filename patterns for all locations
    print("\n🔍 7. Testing filename patterns...")
    
    # Based on the working file, extract the pattern
    working_file = "SupermarketTrg-Grada-Vukovara-8-Velika-Gorica-10410-21.03.2026-7.00h-C30.csv"
    print(f"  Working pattern: {working_file}")
    
    # Parse the working pattern
    pattern_parts = working_file.split('-')
    print(f"  Pattern parts: {pattern_parts}")
    
    # Test variations for different dates
    test_dates = [
        (datetime.now() - timedelta(days=i)).strftime("%d.%m.%Y") 
        for i in range(3)
    ]
    
    # Try to find other files by guessing city names
    test_locations = [
        ("Velika Gorica", "SupermarketTrg-Grada-Vukovara-8-Velika-Gorica-10410"),
        ("Zagreb", "Supermarket-Ilica-123-Zagreb-10000"),
        ("Zagreb", "Supermarket-Savska-45-Zagreb-10000"),
        ("Split", "Supermarket-Vukovarska-12-Split-21000"),
        ("Rijeka", "Supermarket-Korzo-23-Rijeka-51000"),
        ("Osijek", "Supermarket-Europske-Avenije-78-Osijek-31000"),
        ("Zadar", "Supermarket-Poluotok-5-Zadar-23000"),
        ("Pula", "Supermarket-Istarska-34-Pula-52100"),
    ]
    
    print("\n  Testing filename patterns for different dates:")
    for city, identifier in test_locations:
        for test_date in test_dates:
            test_filename = f"{identifier}-{test_date}-7.00h-C30.csv"
            url = f"{base_url}/wp-content/uploads/2026/03/{test_filename}"
            try:
                r = requests.head(url, timeout=5)
                if r.status_code == 200:
                    print(f"  ✓ EXISTS: {test_filename}")
                    discovered_data["csv_files"].append(url)
                else:
                    print(f"  ✗ MISSING: {test_filename}")
            except:
                print(f"  ✗ ERROR checking: {test_filename}")
    
    # 8. Check for any JavaScript that might contain location data
    print("\n📜 8. Scanning JavaScript for location data...")
    try:
        r = requests.get(f"{base_url}/cjenik/", timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and ('location' in script.string.lower() or 'cjenik' in script.string.lower()):
                # Look for arrays or objects
                locations_array = re.findall(r'locations\s*:\s*\[([^\]]+)\]', script.string, re.IGNORECASE)
                if locations_array:
                    print(f"  Found locations array: {locations_array[0][:200]}")
                
                # Look for hardcoded locations
                location_names = re.findall(r'["\']([^"\']+?(?:Zagreb|Split|Rijeka|Osijek)[^"\']*?)["\']', script.string)
                if location_names:
                    for loc in set(location_names):
                        if len(loc) > 5 and ('Supermarket' in loc or 'Trg' in loc):
                            print(f"  Found location reference: {loc}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("📊 SCAN SUMMARY")
    print("="*80)
    print(f"  Locations found: {len(discovered_data['locations'])}")
    print(f"  CSV files found: {len(discovered_data['csv_files'])}")
    print(f"  Upload directories: {len(discovered_data['upload_directories'])}")
    print(f"  API endpoints: {len(discovered_data['api_endpoints'])}")
    
    if discovered_data['csv_files']:
        print("\n  CSV Files discovered:")
        for csv_file in discovered_data['csv_files'][:10]:  # Show first 10
            print(f"    - {csv_file}")
    
    # Save results to file
    with open('zabac_discovery.json', 'w', encoding='utf-8') as f:
        json.dump(discovered_data, f, indent=2, ensure_ascii=False)
    print(f"\n  ✅ Full results saved to zabac_discovery.json")
    
    return discovered_data

# Run the deep scan
if __name__ == "__main__":
    results = deep_scan_zabac()
    
    # Based on results, suggest what to do next
    print("\n" + "="*80)
    print("💡 RECOMMENDATIONS")
    print("="*80)
    
    if results['csv_files']:
        print("✅ Found CSV files! Use these exact filenames.")
        print("   The downloader should use these specific files.")
    elif results['locations']:
        print("📍 Found locations but no CSV files.")
        print("   The website might generate CSV on demand via AJAX.")
        print("   Need to replicate the AJAX request that downloads the file.")
    else:
        print("⚠️ No CSV files or locations found directly.")
        print("   The website might use JavaScript to load data dynamically.")
        print("   Consider using Selenium or Playwright to interact with the page.")