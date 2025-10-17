#!/usr/bin/env python3
"""
Render Web Service FastDL Scraper

- ord_ prefix maps
- batch progress every 5000 attempts
- Discord webhook notifications
- HTTP server for Render health checks
"""

import os
import asyncio
import string
from itertools import product
from urllib.parse import urljoin
from datetime import datetime
import aiohttp
import bz2
import json
import threading
from aiohttp import web

# ================= CONFIG =================
BASE_URL = "http://169.150.249.133/fastdl/teamfortress2/679d9656b8573d37aa848d60/maps/"
ARCHIVE_DIR = "archive"
CONCURRENCY = 80
DELAY = 0.0
EXTENSIONS = [".bsp.bz2", ".bsp"]
CHARSET = string.ascii_lowercase + string.digits
TIMEOUT = 20
BATCH_SIZE = 5000
CHECKPOINT_FILE = "checkpoint.json"
STATUS_INTERVAL = 20 * 60  # 20 minutes
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
MAP_LENGTH = int(os.getenv("MAP_LENGTH", "5"))
# ==========================================

os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ---------- Utility ----------
def timestamped_name(name: str) -> str:
    now = datetime.now()
    base, ext = os.path.splitext(name)
    ts = f"{now.day:02d}{now.month:02d}{now.year}{now.hour:02d}{now.minute:02d}{now.second:02d}"
    return f"{ts}_{base}{ext}"

# ---------- Checkpoint ----------
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except:
            return {"count":0,"batch":0}
    return {"count":0,"batch":0}

def save_checkpoint(counter):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(counter, f)

# ---------- Discord ----------
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
async def try_name(session: aiohttp.ClientSession, name: str):
    full_base = f"ord_{name}"
    for ext in EXTENSIONS:
        filename = f"{full_base}{ext}"
        url = urljoin(BASE_URL, filename)
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    local_path = os.path.join(ARCHIVE_DIR, filename)
                    with open(local_path, "wb") as f:
                        f.write(data)

                    # decompress .bz2
                    if local_path.endswith(".bz2"):
                        out_path = local_path[:-4]
                        with open(local_path, "rb") as f:
                            decompressed = bz2.decompress(f.read())
                        with open(out_path, "wb") as f:
                            f.write(decompressed)
                        os.remove(local_path)
                        local_path = out_path
                        filename = filename[:-4]

                    final_name = timestamped_name(os.path.basename(local_path))
                    os.rename(local_path, os.path.join(ARCHIVE_DIR, final_name))
                    print(f"[+] FOUND MAP: {filename} → archived as {final_name}")
                    await notify_discord(f"✅ FOUND MAP: {filename}", os.path.join(ARCHIVE_DIR, final_name))
        except Exception:
            pass
        if DELAY > 0:
            await asyncio.sleep(DELAY)

async def worker(name_queue: asyncio.Queue, session: aiohttp.ClientSession, counter, total_combinations):
    while not name_queue.empty():
        name = await name_queue.get()
        await try_name(session, name)
        counter["count"] += 1

        if counter["count"] % BATCH_SIZE == 0:
            counter["batch"] += 1
            print(f"✅ Completed Batch #{counter['batch']} ({counter['count']:,} attempts)")
            save_checkpoint(counter)

        name_queue.task_done()

# ---------- Status Notifier ----------
async def status_notifier(counter, total_combinations):
    while True:
        await asyncio.sleep(STATUS_INTERVAL)
        percent = (counter["count"]/total_combinations)*100
        msg = f"⏱ Scraping progress: {percent:.2f}% ({counter['count']:,}/{total_combinations:,})"
        print(msg)
        await notify_discord(msg)

# ---------- Main Scraper ----------
async def scraper_main():
    counter = load_checkpoint()
    name_queue = asyncio.Queue()
    total_combinations = len(CHARSET) ** MAP_LENGTH

    current_index = 0
    for combo in product(CHARSET, repeat=MAP_LENGTH):
        if current_index >= counter["count"]:
            await name_queue.put("".join(combo))
        current_index += 1

    async with aiohttp.ClientSession() as session:
        asyncio.create_task(status_notifier(counter, total_combinations))
        tasks = [asyncio.create_task(worker(name_queue, session, counter, total_combinations)) for _ in range(CONCURRENCY)]
        await name_queue.join()
        for t in tasks:
            t.cancel()

    save_checkpoint(counter)
    completion_msg = f"🎉 Scraping finished for character length {MAP_LENGTH}! Total attempts: {counter['count']:,}, Total batches: {counter['batch']}"
    print(completion_msg)
    await notify_discord(completion_msg)

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
    # Run scraper in background thread
    import threading
    threading.Thread(target=start_scraper, daemon=True).start()
    # Start web service for Render health
    start_web_service()