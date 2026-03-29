import os
import re
import time
import requests
from supabase import create_client

# Connect to Supabase
def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

NTFY_TOPIC = "discord-tracker-maciej-g57gt683jg730ds"

def send_alert(title, message, priority="default"):
    """Send notification via ntfy.sh"""
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": priority
            },
            timeout=10
        )
    except Exception as e:
        print(f"Failed to send alert: {e}")

DISCORD_INVITE_PATTERN = re.compile(
    r'(?:discord\.gg(?:/|%2F)|discord\.com(?:/|%2F)invite(?:/|%2F))([a-zA-Z0-9]+)'
)

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
            if not items:
                print("  No more items, stopping")
                break

            for item in items:
                name = item.get("name")
                logo = item.get("logo", "")
                # Extract app ID from logo URL e.g. /apps/730/capsule
                match = re.search(r'/apps/(\d+)/', logo)
                if match and name:
                    app_id = int(match.group(1))
                    if app_id not in seen_ids:
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
            headers={"User-Agent": "Mozilla/5.0"},
            cookies={
                "birthtime": "631152001",  # bypasses age gate
                "lastagecheckage": "1-0-2000",
                "wants_mature_content": "1"  # bypasses mature content gate
            }
        )
        match = DISCORD_INVITE_PATTERN.search(response.text)
        if match:
            return match.group(1)
    except Exception as e:
        print(f"Error fetching Steam page for {steam_app_id}: {e}")
    return None

def upsert_game(game, steam_list, rank, retries=3):
    for attempt in range(retries):
        try:
            get_supabase().table("games").upsert({
                "steam_app_id": game["steam_app_id"],
                "name": game["name"],
                "steam_list": steam_list,
                "steam_rank": rank,
                "is_released": steam_list == "most_played"
            }, on_conflict="steam_app_id").execute()
            return
        except Exception as e:
            print(f"  DB error on upsert_game attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(5)
            else:
                print(f"  Giving up on {game['name']}, continuing...")

def upsert_discord_server(game_id, invite_code, retries=3):
    for attempt in range(retries):
        try:
            # Check if this game_id + invite_code combo already exists
            existing = get_supabase().table("discord_servers")\
                .select("id")\
                .eq("game_id", game_id)\
                .eq("invite_code", invite_code)\
                .execute()
            
            if existing.data:
                # Already exists, skip
                return
            
            get_supabase().table("discord_servers").insert({
                "game_id": game_id,
                "invite_code": invite_code,
                "is_active": True
            }).execute()
            return
        except Exception as e:
            print(f"  DB error on upsert_discord_server attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(5)
            else:
                print(f"  Giving up on invite {invite_code}, continuing...")

def process_games(games, steam_list):
    """Process a list of games - upsert them and find their Discord servers"""
    print(f"Processing {len(games)} {steam_list} games...")

    # Get steam_app_ids that already have a discord server linked
    linked_ids = set()
    offset = 0
    batch_size = 1000

    while True:
        linked_result = get_supabase().table("discord_servers")\
            .select("game_id, games(steam_app_id)")\
            .range(offset, offset + batch_size - 1)\
            .execute()
        
        batch = linked_result.data
        if not batch:
            break
            
        for row in batch:
            if row.get("games"):
                linked_ids.add(row["games"]["steam_app_id"])
        
        if len(batch) < batch_size:
            break
            
        offset += batch_size

    for rank, game in enumerate(games, 1):
        steam_app_id = game["steam_app_id"]

        # Always upsert the game itself (fast, no Steam page request)
        upsert_game(game, steam_list, rank)

        # Skip Steam page scrape if we already found a Discord for this game
        if steam_app_id in linked_ids:
            print(f"[{rank}/{len(games)}] {game['name']} - already has Discord, skipping")
            continue

        print(f"[{rank}/{len(games)}] {game['name']} ({steam_app_id})")

        # Get game ID from database
        result = get_supabase().table("games")\
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
    try:
        played_games = get_steam_games("mostplayed", 1000)
        process_games(played_games, "most_played")

        wishlisted_games = get_steam_games("popularwishlist", 4000)
        process_games(wishlisted_games, "most_wishlisted")

        total_games = len(played_games) + len(wishlisted_games)
        discord_result = get_supabase().table("discord_servers")\
            .select("id", count="exact")\
            .eq("is_active", True)\
            .execute()
        active_servers = discord_result.count

        send_alert(
            "Steam Scraper Complete",
            f"Games processed: {total_games} | Active Discord servers: {active_servers}"
        )

    except Exception as e:
        send_alert(
            "Steam Scraper Failed",
            f"Error: {str(e)}",
            priority="high"
        )
        raise
