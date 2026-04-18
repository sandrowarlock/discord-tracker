import os
import re
import time
import requests
from supabase import create_client

def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

DISCORD_INVITE_PATTERN = re.compile(
    r'(?:discord\.gg(?:/|%2F)|discord\.com(?:/|%2F)invite(?:/|%2F))([a-zA-Z0-9-]+)'
)

def get_games_without_discord():
    """Fetch all games that don't have a Discord server linked"""
    result = get_supabase().rpc("games_without_discord").execute()
    return result.data

def get_discord_invite(steam_app_id):
    """Scrape the Steam store page for a Discord invite link"""
    try:
        response = requests.get(
            f"https://store.steampowered.com/app/{steam_app_id}",
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
            cookies={
                "birthtime": "631152001",
                "lastagecheckage": "1-0-2000",
                "wants_mature_content": "1"
            }
        )
        match = DISCORD_INVITE_PATTERN.search(response.text)
        if match:
            return match.group(1)
    except Exception as e:
        print(f"  Error fetching Steam page: {e}")
    return None

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

if __name__ == "__main__":
    # Get all games without discord directly via SQL
    result = get_supabase().table("games")\
        .select("id, steam_app_id, name, steam_rank, steam_list")\
        .execute()
    
    all_games = result.data
    
    # Get game_ids that already have an active discord server
    linked = get_supabase().table("discord_servers")\
        .select("game_id")\
        .eq("is_active", True)\
        .execute()
    linked_ids = {row["game_id"] for row in linked.data}
    
    # Also get game_ids with dead invites to re-check
    dead = get_supabase().table("discord_servers")\
        .select("game_id")\
        .eq("inactive_reason", "dead_invite")\
        .execute()
    dead_ids = {row["game_id"] for row in dead.data}
    
    # Filter to games without active discord OR with dead invites
    missing = [g for g in all_games if g["id"] not in linked_ids or g["id"] in dead_ids]
    missing.sort(key=lambda x: (x["steam_list"] != "most_played", x["steam_rank"] or 9999))
    
    print(f"Found {len(missing)} games to re-check")
    print(f"Starting re-scrape...")

    found_count = 0

    for i, game in enumerate(missing, 1):
        steam_app_id = game["steam_app_id"]
        print(f"[{i}/{len(missing)}] {game['name']} ({steam_app_id})")

        invite_code = get_discord_invite(steam_app_id)
        if invite_code:
            print(f"  Found Discord: discord.gg/{invite_code}")
            upsert_discord_server(game["id"], invite_code)
            found_count += 1
        else:
            print(f"  No Discord found")

        time.sleep(1)

    print(f"\nDone! Found {found_count} new Discord servers out of {len(missing)} games checked")
