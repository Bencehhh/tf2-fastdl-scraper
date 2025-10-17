#!/usr/bin/env python3
"""
Render Web Service FastDL Scraper (Single Scraper Instance)

✅ Features:
- ord_ prefix map scraping
- Uses word lists (English dictionaries)
- Only one scraper runs at a time
- GUI to configure map length & wordlist
- Progress display
- Discord webhook notifications (found maps + progress)
- Low memory footprint
- Graceful restart without duplicate threads
"""

import os
import asyncio
import aiohttp
import bz2
import json
import threading
from aiohttp import web
from datetime import datetime
from urllib.parse import urljoin

# ================= CONFIG =================
BASE_URL = "http://169.150.249.133/fastdl/teamfortress2/679d9656b8573d37aa848d60/maps/"
ARCHIVE_DIR = "archive"
EXTENSIONS = [".bsp.bz2", ".bsp"]
CONCURRENCY = 5
TIMEOUT = 20
BATCH_SIZE = 5000
STATUS_INTERVAL = 20 * 60  # 20 minutes
CHECKPOINT_FILE = "checkpoint.json"

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
DEFAULT_WORDLIST = "words_alpha.txt"
DEFAULT_MAP_LENGTH = int(os.getenv("MAP_LENGTH", "4"))
PORT = int(os.getenv("PORT", "10000"))
# ==========================================

os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ---------- Globals ----------
status = {
    "map_length": DEFAULT_MAP_LENGTH,
    "wordlist": DEFAULT_WORDLIST,
    "total": 0,
    "count": 0,
    "batch": 0,
    "running": False,
    "current_word": "",
    "found_maps": []
}

stop_event = threading.Event()
scraper_thread = None


# ---------- Utilities ----------
def timestamped_name(name: str) -> str:
    now = datetime.now()
    base, ext = os.path.splitext(name)
    ts = f"{now.day:02d}{now.month:02d}{now.year}{now.hour:02d}{now.minute:02d}{now.second:02d}"
    return f"{ts}_{base}{ext}"


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


async def notify_discord(message: str, file_path: str = None):
    if not DISCORD_WEBHOOK:
        return
    try:
        async with aiohttp.ClientSession() as session:
            if file_path and os.path.exists(file_path):
                form = aiohttp.FormData()
                form.add_field('file', open(file_path, 'rb'), filename=os.path.basename(file_path))
                form.add_field('content', message)
                await session.post(DISCORD_WEBHOOK, data=form)
            else:
                await session.post(DISCORD_WEBHOOK, json={"content": message})
    except:
        pass


# ---------- Scraper ----------
async def try_word(session: aiohttp.ClientSession, word: str):
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
                    msg = f"✅ FOUND MAP: {filename}"
                    print(msg)
                    status["found_maps"].append(filename)
                    await notify_discord(msg, final_path)
        except:
            pass


async def scraper_main():
    counter = load_checkpoint()

    # Load selected word list
    wordlist_file = status["wordlist"]
    map_length = status["map_length"]
    with open(wordlist_file, "r") as f:
        words = [w.strip() for w in f if len(w.strip()) == map_length]

    total_words = len(words)
    status["total"] = total_words
    status["count"] = counter["count"]
    status["batch"] = counter["batch"]
    print(f"[+] Total {total_words} English words of length {map_length}")

    status["running"] = True

    async with aiohttp.ClientSession() as session:

        async def status_notifier():
            while status["running"] and not stop_event.is_set():
                await asyncio.sleep(STATUS_INTERVAL)
                percent = (status["count"] / total_words) * 100
                msg = f"⏱ Progress: {percent:.2f}% ({status['count']:,}/{total_words:,})"
                print(msg)
                await notify_discord(msg)

        asyncio.create_task(status_notifier())
        semaphore = asyncio.Semaphore(CONCURRENCY)

        for i in range(counter.get("index", 0), total_words):
            if stop_event.is_set():
                print("[*] Stop event received. Exiting scraper.")
                break
            word = words[i]
            status["current_word"] = word
            async with semaphore:
                await try_word(session, word)
            status["count"] += 1
            counter["count"] = status["count"]
            counter["index"] = i + 1

            if status["count"] % BATCH_SIZE == 0:
                status["batch"] += 1
                counter["batch"] = status["batch"]
                save_checkpoint(counter)
                print(f"✅ Completed Batch #{status['batch']} ({status['count']:,} attempts)")

    save_checkpoint(counter)
    status["running"] = False
    msg = f"🎉 Scraping finished for length {map_length}! Total attempts: {status['count']:,}, Total batches: {status['batch']}"
    print(msg)
    await notify_discord(msg)


# ---------- Web Server ----------
async def index(request):
    found = "<br>".join(status["found_maps"][-10:]) or "None yet"
    percent = (status["count"] / status["total"] * 100) if status["total"] else 0
    html = f"""
    <html><head><title>TF2 FastDL Scraper</title></head><body>
    <h1>TF2 FastDL Scraper</h1>
    <p>Map Length: {status['map_length']}</p>
    <p>Wordlist: {status['wordlist']}</p>
    <p>Progress: {percent:.2f}% ({status['count']:,}/{status['total']:,})</p>
    <p>Current Word: {status['current_word']}</p>
    <p>Batches Completed: {status['batch']}</p>
    <h3>Recently Found Maps</h3>
    <div style="background:#f0f0f0;padding:10px;">{found}</div>
    <form action="/configure" method="post">
      <p>Map Length: <input type="number" name="map_length" value="{status['map_length']}"></p>
      <p>Wordlist:
        <select name="wordlist">
          {"".join([f'<option value="{w}" {"selected" if w==status["wordlist"] else ""}>{w}</option>' for w in os.listdir(".") if w.endswith(".txt")])}
        </select>
      </p>
      <button type="submit">Restart Scraper</button>
    </form>
    </body></html>
    """
    return web.Response(text=html, content_type="text/html")


async def configure(request):
    data = await request.post()
    map_length = int(data.get("map_length", status["map_length"]))
    wordlist = data.get("wordlist", status["wordlist"])

    status["map_length"] = map_length
    status["wordlist"] = wordlist
    start_scraper()
    return web.HTTPFound('/')


def start_web_service():
    app = web.Application()
    app.add_routes([web.get('/', index), web.post('/configure', configure)])
    web.run_app(app, port=PORT)


# ---------- Scraper Thread ----------
def start_scraper():
    global scraper_thread
    if scraper_thread and scraper_thread.is_alive():
        print("[*] Stopping old scraper thread...")
        stop_event.set()
        scraper_thread.join(timeout=10)

    stop_event.clear()
    scraper_thread = threading.Thread(target=lambda: asyncio.run(scraper_main()), daemon=True)
    scraper_thread.start()
    print("[+] Scraper started")


# ---------- Main ----------
if __name__ == "__main__":
    start_scraper()
    start_web_service()
