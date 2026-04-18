import os
import time
import requests
import csv
from supabase import create_client

def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

DISCORD_API = "https://discord.com/api/v9/invites/{}?with_counts=true"

def get_active_servers():
    """Fetch all active Discord servers with their stored guild_id"""
    all_servers = []
    offset = 0
    batch_size = 1000

    while True:
        result = get_supabase().table("discord_servers")\
            .select("id, invite_code, guild_id, guild_name, game_id")\
            .eq("is_active", True)\
            .range(offset, offset + batch_size - 1)\
            .execute()

        batch = result.data
        if not batch:
            break

        all_servers.extend(batch)
        if len(batch) < batch_size:
            break

        offset += batch_size

    return all_servers

def get_game_names():
    """Fetch game names keyed by id"""
    all_games = {}
    offset = 0
    batch_size = 1000

    while True:
        result = get_supabase().table("games")\
            .select("id, name")\
            .range(offset, offset + batch_size - 1)\
            .execute()

        batch = result.data
        if not batch:
            break

        for row in batch:
            all_games[row["id"]] = row["name"]

        if len(batch) < batch_size:
            break

        offset += batch_size

    return all_games

def check_invite(invite_code):
    """Hit the Discord invite API and return guild info"""
    for attempt in range(3):
        try:
            response = requests.get(
                DISCORD_API.format(invite_code),
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"}
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "status": "ok",
                    "live_guild_id": data.get("guild", {}).get("id"),
                    "live_guild_name": data.get("guild", {}).get("name"),
                }
            elif response.status_code == 404:
                return {"status": "dead"}
            elif response.status_code == 429:
                retry_after = response.json().get("retry_after", 5)
                print(f"  Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            else:
                return {"status": "error", "code": response.status_code}

        except Exception as e:
            print(f"  Request error attempt {attempt + 1}: {e}")
            if attempt < 2:
                time.sleep(5)

    return {"status": "error"}

if __name__ == "__main__":
    servers = get_active_servers()
    game_names = get_game_names()
    print(f"Checking {len(servers)} active servers...")

    issues = []

    with open("invite_verification.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "server_id", "game_name", "invite_code",
            "stored_guild_id", "stored_guild_name",
            "live_guild_id", "live_guild_name",
            "issue"
        ])
        writer.writeheader()

        for i, server in enumerate(servers, 1):
            server_id = server["id"]
            invite_code = server["invite_code"]
            stored_guild_id = server.get("guild_id")
            stored_guild_name = server.get("guild_name")
            game_name = game_names.get(server["game_id"], "Unknown")

            print(f"[{i}/{len(servers)}] {game_name} - discord.gg/{invite_code}")

            result = check_invite(invite_code)
            issue = None

            if result["status"] == "dead":
                issue = "dead_invite"
                print(f"  DEAD INVITE")

            elif result["status"] == "ok":
                live_guild_id = result["live_guild_id"]
                live_guild_name = result["live_guild_name"]

                # Flag if guild_id mismatch (and we have a stored one to compare)
                if stored_guild_id and live_guild_id != stored_guild_id:
                    issue = "guild_id_mismatch"
                    print(f"  MISMATCH: stored={stored_guild_id} live={live_guild_id}")
                    print(f"  Stored name: {stored_guild_name} | Live name: {live_guild_name}")

                # Flag if invite code looks suspiciously short (possible truncation)
                elif len(invite_code) <= 6 and "-" not in invite_code:
                    issue = "possibly_truncated"
                    print(f"  SHORT CODE: {invite_code} -> {live_guild_name}")

                else:
                    print(f"  OK: {live_guild_name}")

            else:
                issue = "api_error"
                print(f"  ERROR")

            if issue:
                row = {
                    "server_id": server_id,
                    "game_name": game_name,
                    "invite_code": invite_code,
                    "stored_guild_id": stored_guild_id,
                    "stored_guild_name": stored_guild_name,
                    "live_guild_id": result.get("live_guild_id", ""),
                    "live_guild_name": result.get("live_guild_name", ""),
                    "issue": issue
                }
                writer.writerow(row)
                issues.append(row)

            time.sleep(1)

    print(f"\nDone!")
    print(f"  Total checked: {len(servers)}")
    print(f"  Issues found: {len(issues)}")
    print(f"  Results saved to invite_verification.csv")
