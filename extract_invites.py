import csv
import re
import time
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def get_invite_from_discovery_page(url):
    """Fetch a Discord server discovery page and extract the invite code"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            print(f"  Got status {response.status_code}")
            return None

        match = re.search(r'https://discord\.gg/([a-zA-Z0-9-]+)', response.text)
        if match:
            return match.group(1)
        else:
            print(f"  No invite link found in page")
            return None

    except Exception as e:
        print(f"  Error: {e}")
        return None

if __name__ == "__main__":
    # Read URLs from input file
    try:
        with open("server_urls.txt") as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    except FileNotFoundError:
        print("server_urls.txt not found. Please create it with one Discord server URL per line.")
        exit(1)

    print(f"Found {len(urls)} URLs to process")

    results = []
    for i, url in enumerate(urls, 1):
        # Extract guild ID and slug from URL
        match = re.search(r'/servers/(.+)-(\d+)$', url)
        if not match:
            print(f"[{i}/{len(urls)}] Skipping invalid URL: {url}")
            continue

        slug = match.group(1)
        guild_id = match.group(2)
        print(f"[{i}/{len(urls)}] {slug} ({guild_id})")

        invite_code = get_invite_from_discovery_page(url)
        print(f"  invite: {invite_code or 'NOT FOUND'}")

        results.append({
            "guild_id": guild_id,
            "slug": slug,
            "invite_code": invite_code or "",
            "discovery_url": url
        })

        time.sleep(1)

    # Write results
    output_file = "extracted_invites.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["guild_id", "slug", "invite_code", "discovery_url"])
        writer.writeheader()
        writer.writerows(results)

    found = sum(1 for r in results if r["invite_code"])
    print(f"\nDone! Found invite codes for {found}/{len(results)} servers")
    print(f"Results saved to {output_file}")
