"""Bulk download all market data for offline backtesting.

Downloads:
  1. 1m candles from Bybit (229 perp assets)
  2. 1m candles from HL API (146 builder DEX assets)
  3. Funding rates from Bybit (229 perp assets)
  4. Funding rates from HL API (146 builder DEX assets)
  5. Long/short ratios from Bybit (229 perp assets)
  6. (optional) 1m candles from S3 fills (builder DEX)

Usage:
    uv run python scripts/prefetch_candles.py
    uv run python scripts/prefetch_candles.py --months 12
    uv run python scripts/prefetch_candles.py --concurrency 10
    uv run python scripts/prefetch_candles.py --skip-s3  # skip S3 builder DEX
"""

import argparse
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd
from tqdm import tqdm

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
BYBIT_FUNDING_URL = "https://api.bybit.com/v5/market/funding/history"
BYBIT_LS_RATIO_URL = "https://api.bybit.com/v5/market/account-ratio"
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

MAX_CANDLES_PER_REQ = 200
MAX_FUNDING_PER_REQ = 200
MAX_LS_PER_REQ = 500
MS_PER_MINUTE = 60_000

CACHE_DIR = Path("data/cache/candles")
FUNDING_CACHE_DIR = Path("data/cache/funding")
LS_RATIO_CACHE_DIR = Path("data/cache/ls_ratio")

RENAMES = {
    "kPEPE": "1000PEPE", "kBONK": "1000BONK", "kFLOKI": "1000FLOKI",
    "kSHIB": "SHIB1000", "kLUNC": "1000LUNC", "kNEIRO": "1000NEIROCTO",
    "RNDR": "RENDER", "FTM": "SONIC", "MATIC": "POL", "TURBO": "1000TURBO",
    "JELLY": "JELLYJELLY", "HPOS": "HPOS10I", "TST": "TSTBSC",
}

# S3 config
S3_BUCKET = "hl-mainnet-node-data"
S3_REGION = "ap-northeast-1"
_FILLS_BY_BLOCK_START = datetime(2025, 7, 27, tzinfo=timezone.utc)
_NODE_FILLS_START = datetime(2025, 5, 25, tzinfo=timezone.utc)
_NODE_TRADES_START = datetime(2025, 3, 22, tzinfo=timezone.utc)


def hl_to_bybit_symbol(hl_asset: str) -> str | None:
    if ":" in hl_asset:
        return None
    base = RENAMES.get(hl_asset, hl_asset)
    return f"{base}USDT"


def _candle_cache_path(asset: str) -> Path:
    return CACHE_DIR / asset.replace(":", "_") / "1m.parquet"


def _funding_cache_path(asset: str) -> Path:
    return FUNDING_CACHE_DIR / f"{asset.replace(':', '_')}.parquet"


def _ls_cache_path(asset: str) -> Path:
    return LS_RATIO_CACHE_DIR / f"{asset.replace(':', '_')}.parquet"


def _save_parquet(path: Path, new_df: pd.DataFrame, dedup_col: str = "timestamp_ms"):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, new_df], ignore_index=True)
    else:
        df = new_df
    df = df.drop_duplicates(subset=[dedup_col]).sort_values(dedup_col)
    df.to_parquet(path, index=False)
    return len(df)


def _get_cached_range(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["timestamp_ms"])
    if df.empty:
        return None
    return int(df.timestamp_ms.min()), int(df.timestamp_ms.max())


async def _bybit_request(client, url, params, semaphore, max_retries=3):
    async with semaphore:
        for attempt in range(max_retries):
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, httpx.RequestError):
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(1 * (attempt + 1))
    return None


def load_all_assets() -> tuple[list[str], list[str]]:
    """Load assets from snapshot files. Returns (bybit_assets, dex_assets)."""
    all_assets: set[str] = set()

    meta_path = Path("data/proxy_snapshots/meta.json")
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        for u in meta.get("universe", []):
            all_assets.add(u["name"])

    perp_path = Path("data/proxy_snapshots/allPerpMetas.json")
    if perp_path.exists():
        for group in json.loads(perp_path.read_text()):
            for u in group.get("universe", []):
                all_assets.add(u["name"])

    bybit = sorted(a for a in all_assets if hl_to_bybit_symbol(a))
    dex = sorted(a for a in all_assets if not hl_to_bybit_symbol(a))
    return bybit, dex


# ---------------------------------------------------------------------------
# 1. Bybit 1m candles
# ---------------------------------------------------------------------------


async def fetch_bybit_candles(
    asset: str, symbol: str, start_ms: int, end_ms: int,
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, pbar: tqdm,
) -> int:
    cached = _get_cached_range(_candle_cache_path(asset))
    if cached and cached[0] <= start_ms and cached[1] >= end_ms - MS_PER_MINUTE:
        pbar.update(1)
        return 0

    all_rows = []
    cursor_end = end_ms

    while cursor_end > start_ms:
        data = await _bybit_request(client, BYBIT_KLINE_URL, {
            "category": "linear", "symbol": symbol, "interval": "1",
            "start": start_ms, "end": cursor_end, "limit": MAX_CANDLES_PER_REQ,
        }, semaphore)

        if data is None or data.get("retCode") != 0:
            break

        result_list = data.get("result", {}).get("list", [])
        if not result_list:
            break

        for item in result_list:
            ts = int(item[0])
            if start_ms <= ts < end_ms:
                all_rows.append([ts, float(item[1]), float(item[2]),
                                 float(item[3]), float(item[4]), float(item[5])])

        if len(result_list) < MAX_CANDLES_PER_REQ:
            break
        cursor_end = int(result_list[-1][0]) - 1

    if all_rows:
        df = pd.DataFrame(all_rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
        _save_parquet(_candle_cache_path(asset), df)

    pbar.update(1)
    return len(all_rows)


# ---------------------------------------------------------------------------
# 2. S3 fills for builder DEX assets
# ---------------------------------------------------------------------------


def _s3_key_for_hour(dt: datetime) -> tuple[str, str]:
    date_str = dt.strftime("%Y%m%d")
    h = dt.hour
    if dt >= _FILLS_BY_BLOCK_START:
        return "fills_by_block", f"node_fills_by_block/hourly/{date_str}/{h}.lz4"
    if dt >= _NODE_FILLS_START:
        return "node_fills", f"node_fills/hourly/{date_str}/{h}.lz4"
    if dt >= _NODE_TRADES_START:
        return "node_trades", f"node_trades/hourly/{date_str}/{h}.lz4"
    return "none", ""


def _try_decompress(data: bytes) -> str:
    try:
        import lz4.frame
        return lz4.frame.decompress(data).decode("utf-8")
    except Exception:
        return data.decode("utf-8")


def _parse_fills_for_assets(
    raw: str, fmt: str, assets: set[str], start_ms: int, end_ms: int,
) -> dict[str, list[tuple[int, float, float]]]:
    """Parse fills from raw NDJSON for multiple assets at once."""
    result: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    seen: dict[str, set[int]] = defaultdict(set)

    for line in raw.split("\n"):
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        if fmt == "fills_by_block":
            for event in obj.get("events", []):
                if not isinstance(event, list) or len(event) < 2:
                    continue
                f = event[1]
                coin = f.get("coin", "")
                if coin not in assets:
                    continue
                t = f.get("time", 0)
                if t < start_ms or t >= end_ms:
                    continue
                tid = f.get("tid")
                if tid is not None:
                    if tid in seen[coin]:
                        continue
                    seen[coin].add(tid)
                result[coin].append((t, float(f["px"]), float(f["sz"])))

        elif fmt == "node_fills":
            if not isinstance(obj, list) or len(obj) < 2:
                continue
            f = obj[1]
            coin = f.get("coin", "")
            if coin not in assets:
                continue
            t = f.get("time", 0)
            if t < start_ms or t >= end_ms:
                continue
            tid = f.get("tid")
            if tid is not None:
                if tid in seen[coin]:
                    continue
                seen[coin].add(tid)
            result[coin].append((t, float(f["px"]), float(f["sz"])))

        elif fmt == "node_trades":
            coin = obj.get("coin", "")
            if coin not in assets:
                continue
            time_str = obj.get("time", "")
            try:
                dt = datetime.fromisoformat(time_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                t = int(dt.timestamp() * 1000)
            except (ValueError, AttributeError):
                continue
            if t < start_ms or t >= end_ms:
                continue
            result[coin].append((t, float(obj["px"]), float(obj["sz"])))

    return result


def _aggregate_fills_to_1m(
    fills: list[tuple[int, float, float]], start_ms: int, end_ms: int,
) -> list[list]:
    if not fills:
        return []
    buckets: dict[int, list[tuple[int, float, float]]] = defaultdict(list)
    for t, px, sz in fills:
        bucket = (t // MS_PER_MINUTE) * MS_PER_MINUTE
        if start_ms <= bucket < end_ms:
            buckets[bucket].append((t, px, sz))

    rows = []
    for bucket_ms in sorted(buckets):
        bf = sorted(buckets[bucket_ms])
        prices = [p for _, p, _ in bf]
        vols = [s for _, _, s in bf]
        rows.append([bucket_ms, prices[0], max(prices), min(prices), prices[-1], sum(vols)])
    return rows


async def fetch_s3_dex_candles(
    dex_assets: list[str], start_ms: int, end_ms: int, concurrency: int,
) -> dict[str, int]:
    """Download S3 hourly files and extract builder DEX 1m candles."""
    try:
        import boto3
    except ImportError:
        print("boto3 not installed — skipping S3 builder DEX prefetch")
        return {}

    try:
        s3 = boto3.client("s3", region_name=S3_REGION)
    except Exception as e:
        print(f"S3 client init failed: {e}")
        return {}

    asset_set = set(dex_assets)

    # Build list of hours to download
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).replace(
        minute=0, second=0, microsecond=0)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)

    hours = []
    cur = start_dt
    while cur < end_dt:
        fmt, key = _s3_key_for_hour(cur)
        if fmt != "none":
            hours.append((cur, fmt, key))
        cur += timedelta(hours=1)

    if not hours:
        print("S3: no hours in range")
        return {}

    print(f"S3: downloading {len(hours)} hourly files for {len(dex_assets)} builder DEX assets")

    sem = asyncio.Semaphore(concurrency)
    accumulated: dict[str, list[list]] = defaultdict(list)  # asset -> candle rows
    pbar = tqdm(total=len(hours), desc="S3 hours", unit="hour")

    async def _process_hour(dt: datetime, fmt: str, key: str):
        async with sem:
            try:
                raw_bytes = await asyncio.to_thread(
                    lambda: s3.get_object(Bucket=S3_BUCKET, Key=key, RequestPayer="requester")["Body"].read()
                )
            except Exception:
                pbar.update(1)
                return

            raw_text = _try_decompress(raw_bytes)
            hour_start = int(dt.timestamp() * 1000)
            hour_end = hour_start + 3_600_000

            fills_by_asset = _parse_fills_for_assets(raw_text, fmt, asset_set, hour_start, hour_end)
            for asset, fills in fills_by_asset.items():
                rows = _aggregate_fills_to_1m(fills, hour_start, hour_end)
                if rows:
                    accumulated[asset].extend(rows)

            pbar.update(1)

    # Process in batches to avoid holding too much in memory
    batch_size = 100
    for i in range(0, len(hours), batch_size):
        batch = hours[i:i + batch_size]
        await asyncio.gather(*[_process_hour(dt, fmt, key) for dt, fmt, key in batch])

        # Flush accumulated data to cache periodically
        for asset, rows in accumulated.items():
            if rows:
                df = pd.DataFrame(rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
                _save_parquet(_candle_cache_path(asset), df)
        counts = {a: len(r) for a, r in accumulated.items()}
        accumulated.clear()

    pbar.close()

    # Final counts
    result = {}
    for asset in dex_assets:
        path = _candle_cache_path(asset)
        if path.exists():
            result[asset] = len(pd.read_parquet(path))
    return result


# ---------------------------------------------------------------------------
# 2b. HL candles for builder DEX assets
# ---------------------------------------------------------------------------

MAX_HL_CANDLES_PER_REQ = 500


async def fetch_hl_candles(
    asset: str, start_ms: int, end_ms: int,
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, pbar: tqdm,
) -> int:
    """Download 1m candles from HL candleSnapshot API for a builder DEX asset."""
    cached = _get_cached_range(_candle_cache_path(asset))
    if cached and cached[0] <= start_ms and cached[1] >= end_ms - MS_PER_MINUTE:
        pbar.update(1)
        return 0

    all_rows = []
    cursor = start_ms

    while cursor < end_ms:
        async with semaphore:
            for attempt in range(3):
                try:
                    resp = await client.post(HL_INFO_URL, json={
                        "type": "candleSnapshot",
                        "req": {
                            "coin": asset, "interval": "1m",
                            "startTime": cursor, "endTime": end_ms,
                        },
                    })
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception:
                    if attempt == 2:
                        data = []
                    await asyncio.sleep(1 * (attempt + 1))

        if not data:
            break

        for c in data:
            ts = c["t"]
            if start_ms <= ts < end_ms:
                all_rows.append([ts, float(c["o"]), float(c["h"]),
                                 float(c["l"]), float(c["c"]), float(c["v"])])

        if len(data) < MAX_HL_CANDLES_PER_REQ:
            break
        cursor = data[-1]["t"] + 1

    if all_rows:
        df = pd.DataFrame(all_rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
        _save_parquet(_candle_cache_path(asset), df)

    pbar.update(1)
    return len(all_rows)


# ---------------------------------------------------------------------------
# 3. Bybit funding
# ---------------------------------------------------------------------------


async def fetch_bybit_funding(
    asset: str, symbol: str, start_ms: int, end_ms: int,
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, pbar: tqdm,
) -> int:
    cached = _get_cached_range(_funding_cache_path(asset))
    if cached and cached[0] <= start_ms and cached[1] >= end_ms - 8 * 3_600_000:
        pbar.update(1)
        return 0

    all_rows = []
    cursor_end = end_ms

    while cursor_end > start_ms:
        data = await _bybit_request(client, BYBIT_FUNDING_URL, {
            "category": "linear", "symbol": symbol,
            "startTime": start_ms, "endTime": cursor_end, "limit": MAX_FUNDING_PER_REQ,
        }, semaphore)

        if data is None or data.get("retCode") != 0:
            break

        result_list = data.get("result", {}).get("list", [])
        if not result_list:
            break

        for item in result_list:
            ts = int(item.get("fundingRateTimestamp", "0"))
            rate = float(item.get("fundingRate", "0"))
            if start_ms <= ts < end_ms:
                all_rows.append([ts, rate])

        if len(result_list) < MAX_FUNDING_PER_REQ:
            break
        cursor_end = int(result_list[-1].get("fundingRateTimestamp", "0")) - 1

    if all_rows:
        df = pd.DataFrame(all_rows, columns=["timestamp_ms", "funding_rate"])
        _save_parquet(_funding_cache_path(asset), df)

    pbar.update(1)
    return len(all_rows)


# ---------------------------------------------------------------------------
# 4. HL funding for builder DEX
# ---------------------------------------------------------------------------


async def fetch_hl_funding(
    asset: str, start_ms: int, end_ms: int,
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, pbar: tqdm,
) -> int:
    cached = _get_cached_range(_funding_cache_path(asset))
    if cached and cached[0] <= start_ms and cached[1] >= end_ms - 8 * 3_600_000:
        pbar.update(1)
        return 0

    all_rows = []
    cursor_start = start_ms

    while cursor_start < end_ms:
        async with semaphore:
            for attempt in range(3):
                try:
                    resp = await client.post(HL_INFO_URL, json={
                        "type": "fundingHistory", "coin": asset,
                        "startTime": cursor_start, "endTime": end_ms,
                    })
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception:
                    if attempt == 2:
                        data = []
                    await asyncio.sleep(1 * (attempt + 1))

        if not data:
            break

        for item in data:
            ts = item.get("time", 0)
            rate = float(item.get("fundingRate", "0"))
            if start_ms <= ts < end_ms:
                all_rows.append([ts, rate])

        if len(data) < 500:
            break
        cursor_start = data[-1].get("time", 0) + 1

    if all_rows:
        df = pd.DataFrame(all_rows, columns=["timestamp_ms", "funding_rate"])
        _save_parquet(_funding_cache_path(asset), df)

    pbar.update(1)
    return len(all_rows)


# ---------------------------------------------------------------------------
# 5. Bybit long/short ratio
# ---------------------------------------------------------------------------


async def fetch_bybit_ls_ratio(
    asset: str, symbol: str, start_ms: int, end_ms: int,
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore, pbar: tqdm,
) -> int:
    cached = _get_cached_range(_ls_cache_path(asset))
    if cached and cached[0] <= start_ms and cached[1] >= end_ms - 5 * MS_PER_MINUTE:
        pbar.update(1)
        return 0

    all_rows = []
    cursor_end = end_ms

    while cursor_end > start_ms:
        data = await _bybit_request(client, BYBIT_LS_RATIO_URL, {
            "category": "linear", "symbol": symbol, "period": "5min",
            "startTime": start_ms, "endTime": cursor_end, "limit": MAX_LS_PER_REQ,
        }, semaphore)

        if data is None or data.get("retCode") != 0:
            break

        result_list = data.get("result", {}).get("list", [])
        if not result_list:
            break

        for item in result_list:
            ts = int(item.get("timestamp", "0"))
            buy = float(item.get("buyRatio", "0"))
            sell = float(item.get("sellRatio", "0"))
            if start_ms <= ts < end_ms:
                all_rows.append([ts, buy, sell])

        if len(result_list) < MAX_LS_PER_REQ:
            break
        cursor_end = int(result_list[-1].get("timestamp", "0")) - 1

    if all_rows:
        df = pd.DataFrame(all_rows, columns=["timestamp_ms", "buy_ratio", "sell_ratio"])
        _save_parquet(_ls_cache_path(asset), df)

    pbar.update(1)
    return len(all_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main(months: int = 12, concurrency: int = 10, skip_s3: bool = False):
    bybit_assets, dex_assets = load_all_assets()
    if not bybit_assets and not dex_assets:
        print("No assets found. Run: python scripts/collect_snapshots.py")
        return

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - months * 30 * 24 * 60 * MS_PER_MINUTE

    print(f"Period: {months} months ({pd.Timestamp(start_ms, unit='ms')} → {pd.Timestamp(end_ms, unit='ms')})")
    print(f"Bybit assets: {len(bybit_assets)}, Builder DEX assets: {len(dex_assets)}")
    print(f"Concurrency: {concurrency}")
    print()

    semaphore = asyncio.Semaphore(concurrency)
    client = httpx.AsyncClient(timeout=30)

    try:
        bybit_pairs = [(a, hl_to_bybit_symbol(a)) for a in bybit_assets]

        # All steps run in parallel — different endpoints, different cache dirs
        pbar_candles = tqdm(total=len(bybit_pairs), desc="Candles", unit="asset", position=0)
        pbar_funding = tqdm(total=len(bybit_pairs), desc="Funding", unit="asset", position=1)
        pbar_ls = tqdm(total=len(bybit_pairs), desc="L/S Ratio", unit="asset", position=2)
        pbar_hl_funding = tqdm(total=len(dex_assets), desc="HL Funding", unit="asset", position=3) if dex_assets else None
        pbar_hl_candles = tqdm(total=len(dex_assets), desc="HL Candles", unit="asset", position=4) if dex_assets else None

        all_tasks = []

        # 1. Bybit 1m candles
        all_tasks.extend([
            fetch_bybit_candles(a, s, start_ms, end_ms, client, semaphore, pbar_candles)
            for a, s in bybit_pairs
        ])

        # 2. HL 1m candles for builder DEX
        if dex_assets:
            all_tasks.extend([
                fetch_hl_candles(a, start_ms, end_ms, client, semaphore, pbar_hl_candles)
                for a in dex_assets
            ])

        # 3. Bybit funding
        all_tasks.extend([
            fetch_bybit_funding(a, s, start_ms, end_ms, client, semaphore, pbar_funding)
            for a, s in bybit_pairs
        ])

        # 4. Bybit long/short ratio
        all_tasks.extend([
            fetch_bybit_ls_ratio(a, s, start_ms, end_ms, client, semaphore, pbar_ls)
            for a, s in bybit_pairs
        ])

        # 5. HL funding for builder DEX
        if dex_assets:
            all_tasks.extend([
                fetch_hl_funding(a, start_ms, end_ms, client, semaphore, pbar_hl_funding)
                for a in dex_assets
            ])

        print(f"Running {len(all_tasks)} tasks in parallel (semaphore={concurrency})...\n")
        results = await asyncio.gather(*all_tasks)

        pbar_candles.close()
        pbar_funding.close()
        pbar_ls.close()
        if pbar_hl_funding:
            pbar_hl_funding.close()
        if pbar_hl_candles:
            pbar_hl_candles.close()

        # 6. S3 builder DEX candles (optional, separate — uses boto3, not httpx)
        if dex_assets and not skip_s3:
            print(f"\nS3 builder DEX candles ({len(dex_assets)} assets)")
            s3_results = await fetch_s3_dex_candles(dex_assets, start_ms, end_ms, concurrency)
            total_dex = sum(s3_results.values())
            print(f"  → {total_dex:,} candles across {len(s3_results)} assets")
        elif skip_s3:
            print(f"\nS3 builder DEX — skipped (--skip-s3)")

    finally:
        await client.aclose()

    # Summary
    candle_size = sum(f.stat().st_size for f in CACHE_DIR.rglob("1m.parquet")) if CACHE_DIR.exists() else 0
    funding_size = sum(f.stat().st_size for f in FUNDING_CACHE_DIR.rglob("*.parquet")) if FUNDING_CACHE_DIR.exists() else 0
    ls_size = sum(f.stat().st_size for f in LS_RATIO_CACHE_DIR.rglob("*.parquet")) if LS_RATIO_CACHE_DIR.exists() else 0
    total = candle_size + funding_size + ls_size

    print(f"=== Cache Summary ===")
    print(f"  Candles:   {candle_size / 1e9:.2f} GB")
    print(f"  Funding:   {funding_size / 1e6:.1f} MB")
    print(f"  L/S Ratio: {ls_size / 1e6:.1f} MB")
    print(f"  Total:     {total / 1e9:.2f} GB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefetch all market data for offline backtesting")
    parser.add_argument("--months", type=int, default=12, help="Months of history (default: 12)")
    parser.add_argument("--concurrency", type=int, default=10, help="Parallel requests (default: 10)")
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 builder DEX download")
    args = parser.parse_args()
    asyncio.run(main(months=args.months, concurrency=args.concurrency, skip_s3=args.skip_s3))
