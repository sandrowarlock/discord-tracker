import time
import csv
import re
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://discord.com/servers/gaming"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

def scrape_page(offset):
    """Scrape a single page of Discord server discovery"""
    url = BASE_URL if offset == 0 else f"{BASE_URL}?offset={offset}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"  Got status {response.status_code}, skipping")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        servers = []

        # Each server is an anchor tag with /servers/ in the href
        for card in soup.find_all("a", href=re.compile(r"^/servers/[^/]+-\d+$")):
            href = card.get("href", "")

            # Extract guild ID from URL slug (last part after final dash)
            match = re.search(r"-(\d+)$", href)
            if not match:
                continue
            guild_id = match.group(1)

            # Extract invite code from slug (everything between /servers/ and the guild ID)
            slug = href.replace("/servers/", "")
            invite_code = re.sub(r"-\d+$", "", slug)

            # Get all text from the card
            text = card.get_text(" ", strip=True)

            # Check for verified/partnered status
            is_verified = "Verified" in text
            is_partnered = "Partnered" in text

            # Extract server name (first heading or strong tag)
            name_tag = card.find(["h3", "h4", "h5", "strong"])
            name = name_tag.get_text(strip=True) if name_tag else ""

            # Extract member count and online count using regex
            members_match = re.search(r"([\d,]+)\s*Members", text)
            online_match = re.search(r"([\d,]+)\s*Online", text)
            members = members_match.group(1).replace(",", "") if members_match else ""
            online = online_match.group(1).replace(",", "") if online_match else ""

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

    except Exception as e:
        print(f"  Error scraping offset {offset}: {e}")
        return []

if __name__ == "__main__":
    output_file = "discord_discovery.csv"
    total_found = 0
    verified_or_partnered = 0

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "guild_id", "name", "invite_code", "is_verified",
            "is_partnered", "member_count", "online_count", "url"
        ])
        writer.writeheader()

        for offset in range(0, 1800, 12):
            page = (offset // 12) + 1
            print(f"Page {page}/150 (offset={offset})")

            # Wait for content to load
            time.sleep(3)

            servers = scrape_page(offset)

            if not servers:
                print(f"  No servers found, might be end of results")
                # Try one more time before giving up
                time.sleep(5)
                servers = scrape_page(offset)
                if not servers:
                    print(f"  Still nothing, stopping")
                    break

            for server in servers:
                writer.writerow(server)
                if server["is_verified"] or server["is_partnered"]:
                    verified_or_partnered += 1

            total_found += len(servers)
            print(f"  Found {len(servers)} servers ({verified_or_partnered} verified/partnered so far)")

    print(f"\nDone! Total servers: {total_found}")
    print(f"Verified or Partnered: {verified_or_partnered}")
    print(f"Results saved to {output_file}")
