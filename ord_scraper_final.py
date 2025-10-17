#!/usr/bin/env python3
import os
import asyncio
from urllib.parse import urljoin
from datetime import datetime
import aiohttp
import bz2
import json
from aiohttp import web
import threading

# ================= CONFIG =================
BASE_URL = "http://169.150.249.133/fastdl/teamfortress2/679d9656b8573d37aa848d60/maps/"
ARCHIVE_DIR = "archive"
CONCURRENCY = 5
DELAY = 0.0
EXTENSIONS = [".bsp.bz2", ".bsp"]
TIMEOUT = 20
BATCH_SIZE = 5000
CHECKPOINT_FILE = "checkpoint.json"
STATUS_INTERVAL = 20 * 60  # 20 minutes
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
DEFAULT_MAP_LENGTH = int(os.getenv("MAP_LENGTH", "5"))
DEFAULT_WORDLIST = os.getenv("WORDLIST", "words_alpha.txt")
# ==========================================

os.makedirs(ARCHIVE_DIR, exist_ok=True)

# Shared status
status = {
    "current_word": "",
    "attempts": 0,
    "batches": 0,
    "total": 0,
    "map_length": DEFAULT_MAP_LENGTH,
    "running": False,
    "start_time": None,
    "wordlist": DEFAULT_WORDLIST,
}

# Scraper control event
stop_event = asyncio.Event()

# ---------- Checkpoint ----------
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except:
            return {"count": 0, "batch": 0, "index": 0}
    return {"count": 0, "batch": 0, "index": 0}

def save_checkpoint(counter):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(counter, f)

# ---------- Timestamped filenames ----------
def timestamped_name(name: str) -> str:
    now = datetime.now()
    base, ext = os.path.splitext(name)
    ts = f"{now.day:02d}{now.month:02d}{now.year}{now.hour:02d}{now.minute:02d}{now.second:02d}"
    return f"{ts}_{base}{ext}"

# ---------- Discord Notifications ----------
async def notify_discord(message: str, file_path: str = None):
    if not DISCORD_WEBHOOK:
        return
    try:
        async with aiohttp.ClientSession() as session:
            if file_path and os.path.exists(file_path):
                form = aiohttp.FormData()
                form.add_field('file', open(file_path, 'rb'), filename=os.path.basename(file_path))
                form.add_field('content', message)
                resp = await session.post(DISCORD_WEBHOOK, data=form)
            else:
                resp = await session.post(DISCORD_WEBHOOK, json={"content": message})
            if resp.status == 429:
                data = await resp.json()
                retry_after = data.get("retry_after", 1)
                await asyncio.sleep(retry_after)
                await notify_discord(message, file_path)
    except Exception as e:
        print(f"[!] Discord notification failed: {e}")

# ---------- Scraper ----------
async def try_word(session: aiohttp.ClientSession, word: str):
    status["current_word"] = word
    full_base = f"ord_{word.lower()}"
    for ext in EXTENSIONS:
        filename = f"{full_base}{ext}"
        url = urljoin(BASE_URL, filename)
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    local_path = os.path.join(ARCHIVE_DIR, filename)
                    with open(local_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
                    if local_path.endswith(".bz2"):
                        out_path = local_path[:-4]
                        with open(local_path, "rb") as fin, open(out_path, "wb") as fout:
                            decompressor = bz2.BZ2Decompressor()
                            for chunk in iter(lambda: fin.read(8192), b""):
                                fout.write(decompressor.decompress(chunk))
                        os.remove(local_path)
                        local_path = out_path
                        filename = filename[:-4]
                    final_name = timestamped_name(os.path.basename(local_path))
                    final_path = os.path.join(ARCHIVE_DIR, final_name)
                    os.rename(local_path, final_path)

                    print(f"[+] FOUND MAP: {filename}")
                    await notify_discord(f"✅ FOUND MAP: {filename}", final_path)
        except:
            pass
        if DELAY > 0:
            await asyncio.sleep(DELAY)

# ---------- Scraper Main ----------
async def scraper_main():
    stop_event.clear()
    counter = {"count": 0, "batch": 0, "index": 0}

    # Load wordlist
    if not os.path.exists(status["wordlist"]):
        print(f"[!] Wordlist file {status['wordlist']} not found!")
        return

    with open(status["wordlist"], "r") as f:
        words = [w.strip() for w in f if len(w.strip()) == status["map_length"]]
    total_words = len(words)
    status["total"] = total_words
    status["attempts"] = 0
    status["batches"] = 0
    status["running"] = True
    status["start_time"] = datetime.now().isoformat()

    print(f"[+] Starting scrape of {total_words:,} words from {status['wordlist']} (length {status['map_length']})")
    await notify_discord(f"🚀 Started scraping {total_words:,} words from `{status['wordlist']}` with length {status['map_length']}")

    async with aiohttp.ClientSession() as session:
        asyncio.create_task(periodic_status())
        semaphore = asyncio.Semaphore(CONCURRENCY)
        for i, word in enumerate(words):
            if stop_event.is_set():
                break
            async with semaphore:
                await try_word(session, word)
            counter["count"] += 1
            status["attempts"] = counter["count"]
            if counter["count"] % BATCH_SIZE == 0:
                counter["batch"] += 1
                status["batches"] = counter["batch"]
                print(f"✅ Completed Batch #{counter['batch']} ({counter['count']:,} attempts)")

    status["running"] = False
    if not stop_event.is_set():
        await notify_discord(f"🎉 Scraping finished for `{status['wordlist']}` length {status['map_length']}! Total attempts: {counter['count']:,}")

async def periodic_status():
    while status["running"] and not stop_event.is_set():
        await asyncio.sleep(STATUS_INTERVAL)
        if status["total"] > 0:
            percent = (status["attempts"] / status["total"]) * 100
            msg = f"⏱ Progress: {percent:.2f}% ({status['attempts']:,}/{status['total']:,})\nWordlist: {status['wordlist']}"
            print(msg)
            await notify_discord(msg)

# ---------- Web UI ----------
def get_wordlists():
    return [f for f in os.listdir('.') if f.endswith('.txt')]

async def dashboard(request):
    percent = (status["attempts"]/status["total"]*100) if status["total"] else 0
    wordlist_options = "\n".join([
        f'<option value="{wl}" {"selected" if wl == status["wordlist"] else ""}>{wl}</option>'
        for wl in get_wordlists()
    ])
    html = f"""
    <html>
      <head>
        <title>TF2 FastDL Scraper</title>
        <meta http-equiv="refresh" content="5">
        <style>
          body {{ font-family: Arial, sans-serif; padding: 20px; background: #111; color: #eee; }}
          h1 {{ color: #4CAF50; }}
          .box {{ background: #222; padding: 10px; border-radius: 5px; margin-top: 10px; }}
          input, select {{ padding: 5px; margin-top: 5px; }}
        </style>
      </head>
      <body>
        <h1>TF2 FastDL Scraper</h1>
        <div class="box">
          <p>🧠 Status: {"Running" if status['running'] else "Stopped"}</p>
          <p>📝 Current word: <b>{status['current_word']}</b></p>
          <p>🚀 Attempts: {status['attempts']:,}/{status['total']:,} ({percent:.2f}%)</p>
          <p>📦 Batches completed: {status['batches']}</p>
          <p>🔠 Map length: {status['map_length']}</p>
          <p>📜 Wordlist: {status['wordlist']}</p>
          <p>⏳ Started: {status['start_time']}</p>
        </div>
        <form action="/configure" method="post" class="box">
          <label>Map length: <input type="number" name="map_length" min="1" max="25" value="{status['map_length']}"></label><br>
          <label>Wordlist:
            <select name="wordlist">{wordlist_options}</select>
          </label><br><br>
          <button type="submit">Restart Scraper</button>
        </form>
      </body>
    </html>
    """
    return web.Response(text=html, content_type="text/html")

# ---------- Web endpoints ----------
async def configure(request):
    data = await request.post()
    map_length = int(data.get("map_length", status["map_length"]))
    wordlist = data.get("wordlist", status["wordlist"])
    status["map_length"] = map_length
    status["wordlist"] = wordlist

    if status["running"]:
        stop_event.set()

    # restart scraper in new thread
    threading.Thread(target=lambda: asyncio.run(scraper_main()), daemon=True).start()
    return web.HTTPFound('/')

def start_web_service():
    app = web.Application()
    app.add_routes([
        web.get('/', dashboard),
        web.post('/configure', configure)
    ])
    PORT = int(os.getenv("PORT", 10000))
    web.run_app(app, port=PORT)

# ---------- Run ----------
if __name__ == "__main__":
    threading.Thread(target=lambda: asyncio.run(scraper_main()), daemon=True).start()
    start_web_service()