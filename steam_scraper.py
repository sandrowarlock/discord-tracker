import os
import re
import time
import requests
from supabase import create_client

# Connect to Supabase
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

DISCORD_INVITE_PATTERN = re.compile(r'discord\.gg/([a-zA-Z0-9]+)')

def get_steam_games(filter_type, limit):
    """Fetch games from Steam search with pagination"""
    print(f"Fetching top {limit} {filter_type} games from Steam...")
    games = []
    seen_ids = set()
    start = 0
    batch_size = 100

    while len(games) < limit:
        try:
            response = requests.get(
                "https://store.steampowered.com/search/results/",
                params={
                    "filter": filter_type,
                    "json": 1,
                    "start": start,
                    "count": batch_size
                },
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30
            )
            data = response.json()
            if data is None:
                print("  Empty response, stopping")
                break

            items = data.get("items", [])
            if items and start == 0:
                print(f"  DEBUG first item: {items[0]}")
            if not items:
                print("  No more items, stopping")
                break

            for item in items:
                app_id = item.get("id")
                name = item.get("name")
                if app_id and app_id not in seen_ids:
                    seen_ids.add(app_id)
                    games.append({
                        "steam_app_id": app_id,
                        "name": name
                    })

            print(f"  Fetched {len(games)} games so far...")
            start += batch_size
            time.sleep(2)

        except Exception as e:
            print(f"  Error at start={start}: {e}")
            break

    return games[:limit]

def get_discord_invite(steam_app_id):
    """Scrape the Steam store page for a Discord invite link"""
    try:
        response = requests.get(
            f"https://store.steampowered.com/app/{steam_app_id}",
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        match = DISCORD_INVITE_PATTERN.search(response.text)
        if match:
            return match.group(1)
    except Exception as e:
        print(f"Error fetching Steam page for {steam_app_id}: {e}")
    return None

def upsert_game(game, steam_list, rank):
    """Insert or update a game in the database"""
    supabase.table("games").upsert({
        "steam_app_id": game["steam_app_id"],
        "name": game["name"],
        "steam_list": steam_list,
        "steam_rank": rank,
        "is_released": steam_list == "most_played"
    }, on_conflict="steam_app_id").execute()

def upsert_discord_server(game_id, invite_code):
    """Insert or update a discord server in the database"""
    supabase.table("discord_servers").upsert({
        "game_id": game_id,
        "invite_code": invite_code,
        "is_active": True
    }, on_conflict="invite_code").execute()

def process_games(games, steam_list):
    """Process a list of games - upsert them and find their Discord servers"""
    print(f"Processing {len(games)} {steam_list} games...")
    for rank, game in enumerate(games, 1):
        steam_app_id = game["steam_app_id"]
        print(f"[{rank}/{len(games)}] {game['name']} ({steam_app_id})")

        # Upsert the game
        upsert_game(game, steam_list, rank)

        # Get game ID from database
        result = supabase.table("games")\
            .select("id")\
            .eq("steam_app_id", steam_app_id)\
            .single()\
            .execute()
        game_id = result.data["id"]

        # Look for Discord link
        invite_code = get_discord_invite(steam_app_id)
        if invite_code:
            print(f"  Found Discord: discord.gg/{invite_code}")
            upsert_discord_server(game_id, invite_code)
        else:
            print(f"  No Discord found")

        time.sleep(1)

if __name__ == "__main__":
    played_games = get_steam_games("mostplayed", 1000)
    process_games(played_games, "most_played")

    wishlisted_games = get_steam_games("popularwishlist", 4000)
    process_games(wishlisted_games, "most_wishlisted")

    print("Done!")
