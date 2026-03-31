import os
import time
import csv
import re
import requests
from bs4 import BeautifulSoup
from supabase import create_client

def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def get_games_without_discord():
    """Fetch all games without an active Discord server"""
    all_games = []
    offset = 0
    batch_size = 1000

    while True:
        result = get_supabase().table("games")\
            .select("id, steam_app_id, name, steam_rank, steam_list")\
            .range(offset, offset + batch_size - 1)\
            .execute()

        batch = result.data
        if not batch:
            break

        all_games.extend(batch)
        if len(batch) < batch_size:
            break

        offset += batch_size

    # Get game_ids that already have an active discord server
    linked = get_supabase().table("discord_servers")\
        .select("game_id")\
        .eq("is_active", True)\
        .execute()
    linked_ids = {row["game_id"] for row in linked.data}

    missing = [g for g in all_games if g["id"] not in linked_ids]
    missing.sort(key=lambda x: (x["steam_list"] != "most_played", x["steam_rank"] or 9999))
    return missing

def search_duckduckgo(query):
    """Search DuckDuckGo and return top results"""
    try:
        response = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15
        )
        soup = BeautifulSoup(response.text, "html.parser")
        results = []

        for result in soup.select(".result__url"):
            url = result.get_text(strip=True)
            if url:
                # DuckDuckGo shows URLs without https:// prefix sometimes
                if not url.startswith("http"):
                    url = "https://" + url
                results.append(url)

        return results[:10]

    except Exception as e:
        print(f"  DuckDuckGo error: {e}")
        return []

def filter_discord_links(urls):
    """Filter URLs that contain discord.com or discord.gg"""
    return [url for url in urls if "discord.com" in url or "discord.gg" in url]

if __name__ == "__main__":
    games = get_games_without_discord()
    print(f"Found {len(games)} games without Discord links")
    print(f"Starting DuckDuckGo search...")

    output_file = "discord_candidates.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "steam_app_id",
            "game_name",
            "steam_rank",
            "steam_list",
            "discord_url"
        ])

        found_count = 0

        for i, game in enumerate(games, 1):
            query = f"{game['name']} discord server"
            print(f"[{i}/{len(games)}] {game['name']}")

            urls = search_duckduckgo(query)
            discord_links = filter_discord_links(urls)

            if discord_links:
                print(f"  Found {len(discord_links)} Discord links:")
                for url in discord_links:
                    print(f"    {url}")
                    writer.writerow([
                        game["steam_app_id"],
                        game["name"],
                        game["steam_rank"],
                        game["steam_list"],
                        url
                    ])
                found_count += 1
            else:
                print(f"  No Discord links found")
                writer.writerow([
                    game["steam_app_id"],
                    game["name"],
                    game["steam_rank"],
                    game["steam_list"],
                    ""
                ])

            time.sleep(2)  # Be polite to DuckDuckGo

    print(f"\nDone! Found Discord candidates for {found_count}/{len(games)} games")
    print(f"Review discord_candidates.csv and add confirmed official servers to Supabase")
