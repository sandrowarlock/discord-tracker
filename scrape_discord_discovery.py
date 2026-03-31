import csv
import time
import requests

OUTPUT_FILE = "discord_discovery.csv"
MAX_PAGES = 150

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://discord.com/servers/gaming",
    "X-Discord-Locale": "en-US",
}

def get_invite_from_server_page(guild_id, slug):
    """Fetch the server's discovery page and extract the invite link"""
    try:
        url = f"https://discord.com/servers/{slug}-{guild_id}"
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            import re
            match = re.search(r'href="(https://discord\.gg/[^"]+)"', response.text)
            if match:
                invite_url = match.group(1)
                return invite_url.replace("https://discord.gg/", "")
    except Exception as e:
        print(f"    Error fetching server page: {e}")
    return None

def get_discovery_page(offset):
    """Fetch a page of gaming servers from Discord's discovery API"""
    try:
        # Category 1 = Gaming on Discord discovery
        response = requests.get(
            "https://discord.com/api/v9/discovery/categories/1/guilds",
            params={"offset": offset, "limit": 12},
            headers=HEADERS,
            timeout=15
        )
        if response.status_code == 200:
            return response.json()
        else:
            print(f"  API returned {response.status_code}")
            return None
    except Exception as e:
        print(f"  Error: {e}")
        return None

if __name__ == "__main__":
    all_servers = {}

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "guild_id", "name", "invite_code", "is_verified",
            "is_partnered", "member_count", "online_count"
        ])
        writer.writeheader()

        for page_num in range(MAX_PAGES):
            offset = page_num * 12
            print(f"Page {page_num + 1}/{MAX_PAGES} (offset={offset})")

            data = get_discovery_page(offset)

            if not data:
                print("  No data returned, stopping")
                break

            guilds = data.get("guilds", [])
            if not guilds:
                print("  No guilds in response, stopping")
                break

            for guild in guilds:
                guild_id = guild.get("id")
                if guild_id in all_servers:
                    continue

                name = guild.get("name", "")
                slug = guild.get("vanity_url_code") or name.lower().replace(" ", "-")
                is_verified = guild.get("verified", False)
                is_partnered = guild.get("partnered", False)
                member_count = guild.get("approximate_member_count", "")
                online_count = guild.get("approximate_presence_count", "")

                print(f"  [{len(all_servers)+1}] {name} - fetching invite...")
                invite_code = get_invite_from_server_page(guild_id, slug)
                print(f"    invite: {invite_code}")

                row = {
                    "guild_id": guild_id,
                    "name": name,
                    "invite_code": invite_code or "",
                    "is_verified": is_verified,
                    "is_partnered": is_partnered,
                    "member_count": member_count,
                    "online_count": online_count
                }

                all_servers[guild_id] = row
                writer.writerow(row)

            print(f"  Total unique so far: {len(all_servers)}")
            time.sleep(2)

    print(f"\nDone! Total unique servers: {len(all_servers)}")
