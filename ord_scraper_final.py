#!/usr/bin/env python3
"""
Render Web Service FastDL Scraper (English Words)
- Scrapes maps with ord_ prefix
- Tries only valid English words of given length
- Prints + sends Discord webhook when map is found
- Sends status updates every 20 min
- Lightweight (no map list stored in memory)
"""

import os
import asyncio
from urllib.parse import urljoin
from datetime import datetime
import aiohttp
import bz2
import json
from aiohttp import web

# ================= CONFIG =================
BASE_URL = "http://169.150.249.133/fastdl/teamfortress2/679d9656b8573d37aa848d60/maps/"
ARCHIVE_DIR = "archive"
CONCURRENCY = 5
DELAY = 0.0
EXTENSIONS = [".bsp.bz2", ".bsp"]
TIMEOUT = 20
BATCH_SIZE = 5000
CHECKPOINT_FILE = "checkpoint.json"
STATUS_INTERVAL = 60 * 20  # every 20 minutes
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
MAP_LENGTH = int(os.getenv("MAP_LENGTH", "5"))
WORD_LIST_FILE = "words_alpha.txt"  # must exist in repo
# ==========================================

os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ---------- Utilities ----------
def timestamped_name(name: str) -> str:
    now = datetime.now()
    base, ext = os.path.splitext(name)
    ts = f"{now:%d%m%Y%H%M%S}"
    return f"{ts}_{base}{ext}"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {"count": 0, "batch": 0, "index": 0}

def save_checkpoint(counter):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(counter, f)

async def notify_discord(message: str, file_path: str = None):
    if not DISCORD_WEBHOOK:
        return
    async with aiohttp.ClientSession() as session:
        try:
            if file_path and os.path.exists(file_path):
                form = aiohttp.FormData()
                form.add_field('file', open(file_path, 'rb'), filename=os.path.basename(file_path))
                form.add_field('content', message)
                await session.post(DISCORD_WEBHOOK, data=form)
            else:
                await session.post(DISCORD_WEBHOOK, json={"content": message})
        except Exception as e:
            print(f"[!] Discord notification failed: {e}")

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
                    # Stream file to disk
                    with open(local_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
                    # Decompress bz2 if needed
                    if local_path.endswith(".bz2"):
                        out_path = local_path[:-4]
                        with open(local_path, "rb") as fin, open(out_path, "wb") as fout:
                            decompressor = bz2.BZ2Decompressor()
                            for chunk in iter(lambda: fin.read(8192), b""):
                                fout.write(decompressor.decompress(chunk))
                        os.remove(local_path)
                        local_path = out_path
                        filename = filename[:-4]
                    # Rename archived file
                    final_name = timestamped_name(os.path.basename(local_path))
                    archived_path = os.path.join(ARCHIVE_DIR, final_name)
                    os.rename(local_path, archived_path)
                    # Log and notify
                    print(f"[+] FOUND MAP: {filename} → archived as {final_name}")
                    await notify_discord(f"✅ Found map: **{filename}**")
        except:
            pass
        if DELAY > 0:
            await asyncio.sleep(DELAY)

async def scraper_main():
    counter = load_checkpoint()

    # Load only words of target length
    with open(WORD_LIST_FILE, "r") as f:
        words = [w.strip() for w in f if len(w.strip()) == MAP_LENGTH]

    total_words = len(words)
    print(f"[+] Total {total_words:,} English words of length {MAP_LENGTH}")

    async with aiohttp.ClientSession() as session:

        async def status_notifier():
            while True:
                await asyncio.sleep(STATUS_INTERVAL)
                percent = (counter["count"] / total_words) * 100
                msg = f"⏱ Progress: {percent:.2f}% ({counter['count']:,}/{total_words:,})"
                print(msg)
                await notify_discord(msg)

        asyncio.create_task(status_notifier())
        semaphore = asyncio.Semaphore(CONCURRENCY)

        for i in range(counter.get("index", 0), total_words):
            word = words[i]
            async with semaphore:
                await try_word(session, word)
            counter["count"] += 1
            counter["index"] = i + 1
            if counter["count"] % BATCH_SIZE == 0:
                counter["batch"] += 1
                print(f"✅ Completed Batch #{counter['batch']} ({counter['count']:,} attempts)")
                save_checkpoint(counter)

    save_checkpoint(counter)
    done_msg = f"🎉 Scraping finished for length {MAP_LENGTH}! Total attempts: {counter['count']:,}"
    print(done_msg)
    await notify_discord(done_msg)

# ---------- Health check Web Server ----------
async def health(request):
    return web.Response(text="✅ Scraper is running")

def start_web_service():
    app = web.Application()
    app.add_routes([web.get('/', health)])
    PORT = int(os.getenv("PORT", 10000))
    web.run_app(app, port=PORT)

# ---------- Background Scraper ----------
def start_scraper():
    asyncio.run(scraper_main())

if __name__ == "__main__":
    import threading
    threading.Thread(target=start_scraper, daemon=True).start()
    start_web_service()
