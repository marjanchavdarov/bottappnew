def get_latest_zip_url():
    """Fetches the URL for today's data archive."""
    try:
        response = requests.get("https://api.cijene.dev/v0/list")
        if response.status_code == 200:
            archives = response.json().get("archives", [])
            if archives:
                # The first one in the list is usually the newest
                latest = archives[0]
                print(f"🔎 Found latest archive from {latest['date']}")
                return latest['url']
    except Exception as e:
        print(f"❌ Failed to fetch archive list: {e}")
    return None

def download_and_process():
    url = get_latest_zip_url()
    if not url: return
    
    print(f"⬇️ Downloading: {url}")
    r = requests.get(url, stream=True)
    with open("today.zip", "wb") as f:
        for chunk in r.iter_content(chunksize=8192):
            f.write(chunk)
            
    process_master_zip("today.zip") # Use your existing processing function
