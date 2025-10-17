#!/usr/bin/env python3
"""
Render Web Service FastDL Scraper (English Words)

- Scrapes maps with ord_ prefix
- Only tries valid English words of a given length
- Batch progress every 5000 attempts
- Minimal memory usage (streaming downloads)
- Discord webhook notifications
- Web server for health checks
"""

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
STATUS_INTERVAL = 60*60  # 60 minutes
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
MAP_LENGTH = int(os.getenv("MAP_LENGTH"))
WORD_LIST_FILE = "words_alpha.txt"  # English word list
# ==========================================

os.makedirs(ARCHIVE_DIR, exist_ok=True)

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
            return {"count":0,"batch":0,"index":0}
    return {"count":0,"batch":0,"index":0}

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
                    # streaming download
                    with open(local_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
                    # streaming bz2 decompression
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
                    os.rename(local_path, os.path.join(ARCHIVE_DIR, final_name))
                    print(f"[+] FOUND MAP: {filename} → archived as {final_name}")
                    await notify_discord(f"✅ FOUND MAP: {filename}", os.path.join(ARCHIVE_DIR, final_name))
        except:
            pass
        if DELAY > 0:
            await asyncio.sleep(DELAY)

async def scraper_main():
    counter = load_checkpoint()

    # Load filtered words of desired length
    with open(WORD_LIST_FILE, "r") as f:
        words = [w.strip() for w in f if len(w.strip()) == MAP_LENGTH]

    total_words = len(words)
    print(f"[+] Total {total_words} English words of length {MAP_LENGTH}")

    async with aiohttp.ClientSession() as session:
        async def status_notifier():
            while True:
                await asyncio.sleep(STATUS_INTERVAL)
                percent = (counter["count"]/total_words)*100
                msg = f"⏱ Scraping progress: {percent:.2f}% ({counter['count']:,}/{total_words:,})"
                print(msg)
                await notify_discord(msg)
        asyncio.create_task(status_notifier())

        semaphore = asyncio.Semaphore(CONCURRENCY)
        for i in range(counter.get("index",0), total_words):
            word = words[i]
            async with semaphore:
                await try_word(session, word)
            counter["count"] +=1
            counter["index"] = i+1
            if counter["count"] % BATCH_SIZE == 0:
                counter["batch"] +=1
                print(f"✅ Completed Batch #{counter['batch']} ({counter['count']:,} attempts)")
                save_checkpoint(counter)

    save_checkpoint(counter)
    msg = f"🎉 Scraping finished for length {MAP_LENGTH}! Total attempts: {counter['count']:,}, Total batches: {counter['batch']}"
    print(msg)
    await notify_discord(msg)

# ---------- Web Server ----------
async def health(request):
    return web.Response(text="Scraper is running")

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