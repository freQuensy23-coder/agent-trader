#!/usr/bin/env python3
"""
Fetch top GDELT news headlines per day for 2024-03-24 to 2026-03-24.
Uses proxy rotation for parallel fetching (~5 min for 2 years).
Saves incrementally to data/news/headlines.json. Supports resume.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import threading
from threading import Lock

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_FILE = PROJECT_DIR / "data" / "news" / "headlines.json"
PROXY_FILE = Path.home() / "Downloads" / "Webshare Proxy List.txt"

START_DATE = datetime(2024, 3, 24)
END_DATE = datetime(2026, 3, 24)

GDELT_URL = (
    "https://api.gdeltproject.org/api/v2/doc/doc"
    "?query=sourcelang:english"
    "&startdatetime={start}"
    "&enddatetime={end}"
    "&mode=artlist"
    "&format=json"
    "&maxrecords=250"
    "&sort=datesort"
)

MAX_RETRIES = 3
REQUEST_INTERVAL = 6  # seconds between requests per proxy

# Thread-safe state
_save_lock = Lock()
_print_lock = Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def load_proxies() -> list[dict]:
    """Load proxies from Webshare format: ip:port:user:pass"""
    proxies = []
    with open(PROXY_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(":")
            if len(parts) == 4:
                ip, port, user, pwd = parts
                proxies.append({
                    "host": ip,
                    "port": port,
                    "user": user,
                    "pass": pwd,
                })
    return proxies


def load_existing() -> dict:
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_to_disk(data: dict) -> None:
    """Write data to disk. MUST be called while holding _save_lock."""
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tid = threading.get_ident()
    tmp = str(OUTPUT_FILE) + f".tmp.{tid}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, str(OUTPUT_FILE))


def fetch_day(date: datetime, proxy: dict) -> list | None:
    """Fetch articles for a single day using a specific proxy."""
    date_str = date.strftime("%Y%m%d")
    url = GDELT_URL.format(
        start=f"{date_str}000000",
        end=f"{date_str}235959",
    )

    proxy_url = "http://{}:{}@{}:{}".format(
        proxy["user"], proxy["pass"], proxy["host"], proxy["port"],
    )
    proxy_handler = urllib.request.ProxyHandler({"https": proxy_url, "http": proxy_url})
    opener = urllib.request.build_opener(proxy_handler)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with opener.open(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
            if not raw.strip():
                return []
            if "Please limit" in raw:
                log(f"  [{date_str}] 429 text on attempt {attempt}, waiting...")
                time.sleep(10 * attempt)
                continue
            body = json.loads(raw)
            articles = body.get("articles", [])
            return [
                {
                    "title": a.get("title", ""),
                    "url": a.get("url", ""),
                    "domain": a.get("domain", ""),
                    "seendate": a.get("seendate", ""),
                }
                for a in articles
            ]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 15 * attempt
                log(f"  [{date_str}] HTTP 429, attempt {attempt}/{MAX_RETRIES}, wait {wait}s")
                time.sleep(wait)
            else:
                log(f"  [{date_str}] HTTP {e.code}, attempt {attempt}/{MAX_RETRIES}")
                time.sleep(5)
        except Exception as e:
            log(f"  [{date_str}] Error attempt {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(5)

    return None


def worker(dates: list[datetime], proxy: dict, data: dict, stats: dict) -> None:
    """Worker that processes a batch of dates using one proxy."""
    for date in dates:
        key = date.strftime("%Y-%m-%d")
        if key in data:
            continue

        articles = fetch_day(date, proxy)

        with _save_lock:
            if articles is None:
                log(f"  FAILED {key}")
                stats["failed"] += 1
            else:
                data[key] = articles
                stats["fetched"] += 1
                if stats["fetched"] % 50 == 0:
                    log(f"Progress: fetched={stats['fetched']} failed={stats['failed']} | {len(data)} days in file")
                _save_to_disk(data)

        time.sleep(REQUEST_INTERVAL)


def main():
    proxies = load_proxies()
    if not proxies:
        print(f"No proxies found in {PROXY_FILE}", file=sys.stderr)
        sys.exit(1)
    log(f"Loaded {len(proxies)} proxies")

    data = load_existing()
    log(f"Resuming with {len(data)} existing days")

    # Build list of days to fetch
    days_to_fetch = []
    current = START_DATE
    while current <= END_DATE:
        key = current.strftime("%Y-%m-%d")
        if key not in data:
            days_to_fetch.append(current)
        current += timedelta(days=1)

    if not days_to_fetch:
        log("All days already fetched!")
        return

    log(f"Need to fetch {len(days_to_fetch)} days using {len(proxies)} proxies")

    # Distribute days across proxies evenly
    batches: list[list[datetime]] = [[] for _ in range(len(proxies))]
    for i, day in enumerate(days_to_fetch):
        batches[i % len(proxies)].append(day)

    stats = {"fetched": 0, "failed": 0}
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=len(proxies)) as executor:
        futures = []
        for i, batch in enumerate(batches):
            if batch:
                futures.append(executor.submit(worker, batch, proxies[i], data, stats))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"Worker error: {e}")

    elapsed = time.time() - start_time
    log(f"\nDone in {elapsed:.0f}s! {len(data)} days in file, {stats['failed']} failed.")
    log(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
