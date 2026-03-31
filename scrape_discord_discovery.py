import csv
import json
import time
import re
from playwright.sync_api import sync_playwright

BASE_URL = "https://discord.com/servers/gaming"
OUTPUT_FILE = "discord_discovery.csv"
MAX_PAGES = 150

def extract_servers_from_page(page):
    """Extract server data from the page's __NEXT_DATA__ JSON blob"""
    try:
        next_data = page.evaluate("() => JSON.parse(document.getElementById('__NEXT_DATA__').textContent)")
        
        # Navigate the JSON to find server listings
        props = next_data.get("props", {})
        page_props = props.get("pageProps", {})
        
        # Try to find guilds in various locations in the JSON
        guilds = []
        
        # Look for guilds in dehydrated state
        dehydrated = page_props.get("dehydratedState", {})
        queries = dehydrated.get("queries", [])
        
        for query in queries:
            data = query.get("state", {}).get("data", {})
            if isinstance(data, dict):
                nodes = data.get("nodes", [])
                if nodes:
                    guilds.extend(nodes)
                    
        # Also try guilds directly
        if not guilds:
            guilds = page_props.get("guilds", [])

        return guilds

    except Exception as e:
        print(f"  Error extracting JSON: {e}")
        return []

def extract_from_html(page):
    """Fallback: extract server data from rendered HTML"""
    servers = []
    
    # Find all server cards
    cards = page.query_selector_all("a[href*='/servers/']")
    
    for card in cards:
        href = card.get_attribute("href") or ""
        if not re.match(r"^/servers/[^/]+-\d+$", href):
            continue
            
        match = re.search(r"-(\d+)$", href)
        if not match:
            continue
            
        guild_id = match.group(1)
        slug = href.replace("/servers/", "")
        invite_code = re.sub(r"-\d+$", "", slug)
        
        text = card.inner_text()
        is_verified = "Verified" in text
        is_partnered = "Partnered" in text
        
        members_match = re.search(r"([\d,]+)\s*Members", text)
        online_match = re.search(r"([\d,]+)\s*Online", text)
        members = members_match.group(1).replace(",", "") if members_match else ""
        online = online_match.group(1).replace(",", "") if online_match else ""
        
        # Get server name
        name_el = card.query_selector("h3, h4, h5, strong")
        name = name_el.inner_text().strip() if name_el else slug
        
        servers.append({
            "guild_id": guild_id,
            "name": name,
            "invite_code": invite_code,
            "is_verified": is_verified,
            "is_partnered": is_partnered,
            "member_count": members,
            "online_count": online,
            "url": f"https://discord.com{href}"
        })
    
    return servers

if __name__ == "__main__":
    all_servers = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "guild_id", "name", "invite_code", "is_verified",
                "is_partnered", "member_count", "online_count", "url"
            ])
            writer.writeheader()
            
            for page_num in range(MAX_PAGES):
                offset = page_num * 12
                url = BASE_URL if offset == 0 else f"{BASE_URL}?offset={offset}"
                
                print(f"Page {page_num + 1}/{MAX_PAGES} (offset={offset})")
                
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    
                    # Wait for server cards to appear
                    page.wait_for_selector("a[href*='/servers/']", timeout=15000)
                    
                    # Small extra wait for full render
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"  Page load error: {e}, skipping")
                    continue
                
                # Try JSON extraction first, fall back to HTML
                servers = extract_from_html(page)
                
                if not servers:
                    print(f"  No servers found on this page, stopping")
                    break
                
                new_count = 0
                for server in servers:
                    if server["guild_id"] not in all_servers:
                        all_servers[server["guild_id"]] = server
                        writer.writerow(server)
                        new_count += 1
                
                print(f"  Found {len(servers)} servers, {new_count} new (total unique: {len(all_servers)})")
                
                if new_count == 0:
                    print("  No new servers, likely reached end of results")
                    break
                
                time.sleep(2)
        
        browser.close()
    
    print(f"\nDone! Total unique servers: {len(all_servers)}")
    print(f"Results saved to {OUTPUT_FILE}")
