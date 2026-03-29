import os
import time
import requests
from datetime import date
from supabase import create_client

# Connect to Supabase
def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

DISCORD_API = "https://discord.com/api/v9/invites/{}?with_counts=true"

def get_active_servers():
    """Fetch all active Discord servers from the database"""
    all_servers = []
    offset = 0
    batch_size = 1000

    while True:
        result = get_supabase().table("discord_servers")\
            .select("id, invite_code, game_id")\
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

def poll_invite(invite_code):
    """Hit the Discord invite API and return member/online counts"""
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
                    "guild_id": data.get("guild", {}).get("id"),
                    "guild_name": data.get("guild", {}).get("name"),
                    "member_count": data.get("approximate_member_count"),
                    "online_count": data.get("approximate_presence_count")
                }

            elif response.status_code == 429:
                retry_after = response.json().get("retry_after", 5)
                print(f"  Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            elif response.status_code == 404:
                return {"status": "dead"}

            else:
                print(f"  Unexpected status {response.status_code}")
                return {"status": "error"}

        except Exception as e:
            print(f"  Request error attempt {attempt + 1}: {e}")
            if attempt < 2:
                time.sleep(5)

    return {"status": "error"}

def save_snapshot(server_id, member_count, online_count, retries=3):
    """Save a daily snapshot to the database"""
    for attempt in range(retries):
        try:
            supabase.table("daily_snapshots").upsert({
                "discord_server_id": server_id,
                "snapshot_date": date.today().isoformat(),
                "member_count": member_count,
                "online_count": online_count
            }, on_conflict="discord_server_id,snapshot_date").execute()
            return
        except Exception as e:
            print(f"  DB error saving snapshot attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(5)

def deactivate_server(server_id, reason, retries=3):
    """Mark a server as inactive"""
    for attempt in range(retries):
        try:
            supabase.table("discord_servers").update({
                "is_active": False,
                "inactive_reason": reason
            }).eq("id", server_id).execute()
            return
        except Exception as e:
            print(f"  DB error deactivating server attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(5)

def update_server_info(server_id, guild_id, guild_name, retries=3):
    """Update guild ID and name if we have them"""
    for attempt in range(retries):
        try:
            supabase.table("discord_servers").update({
                "guild_id": guild_id,
                "guild_name": guild_name,
                "last_checked_at": date.today().isoformat()
            }).eq("id", server_id).execute()
            return
        except Exception as e:
            print(f"  DB error updating server info attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(5)

if __name__ == "__main__":
    servers = get_active_servers()
    print(f"Polling {len(servers)} active Discord servers...")

    ok_count = 0
    dead_count = 0
    deactivated_count = 0
    error_count = 0

    for i, server in enumerate(servers, 1):
        server_id = server["id"]
        invite_code = server["invite_code"]

        print(f"[{i}/{len(servers)}] discord.gg/{invite_code}")

        result = poll_invite(invite_code)

        if result["status"] == "ok":
            member_count = result["member_count"]
            online_count = result["online_count"]
            print(f"  members={member_count}, online={online_count}")

            # Update guild info
            update_server_info(server_id, result["guild_id"], result["guild_name"])

            # Check if server should be deactivated (below 50 online)
            if online_count is not None and online_count < 50:
                print(f"  Online count below 50, deactivating")
                deactivate_server(server_id, "low_members")
                deactivated_count += 1
            else:
                save_snapshot(server_id, member_count, online_count)
                ok_count += 1

        elif result["status"] == "dead":
            print(f"  Invite is dead, deactivating")
            deactivate_server(server_id, "dead_invite")
            dead_count += 1

        else:
            print(f"  Error, skipping")
            error_count += 1

        time.sleep(1)

    print(f"\nDone!")
    print(f"  Snapshots saved: {ok_count}")
    print(f"  Dead invites: {dead_count}")
    print(f"  Deactivated (low members): {deactivated_count}")
    print(f"  Errors: {error_count}")
