"""Fetch historical fills from HyperLiquid S3 and aggregate into OHLCV candles.

S3 bucket: hl-mainnet-node-data (requester-pays, ap-northeast-1)

Three data formats depending on date:
  - node_fills_by_block/hourly/{YYYYMMDD}/{H}  (Jul 27 2025+)   — block-wrapped NDJSON
  - node_fills/hourly/{YYYYMMDD}/{H}           (May 25–Jul 27)  — [addr, fill] NDJSON
  - node_trades/hourly/{YYYYMMDD}/{H}          (Mar 22–May 25)  — trade NDJSON, ISO time
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from loguru import logger
from weave import op

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from agent_trader.data.market import Candle

S3_BUCKET = "hl-mainnet-node-data"
S3_REGION = "ap-northeast-1"

# Format switchover dates (UTC)
_FILLS_BY_BLOCK_START = datetime(2025, 7, 27, tzinfo=timezone.utc)
_NODE_FILLS_START = datetime(2025, 5, 25, tzinfo=timezone.utc)
_NODE_TRADES_START = datetime(2025, 3, 22, tzinfo=timezone.utc)

INTERVAL_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "1d": 86400,
}

_MAX_CONCURRENT_DOWNLOADS = 8


def _get_s3_client() -> "S3Client":
    import boto3

    return boto3.client("s3", region_name=S3_REGION)


def _hours_in_range(start_ms: int, end_ms: int) -> list[datetime]:
    """Return hour-aligned datetimes covering [start_ms, end_ms)."""
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)

    hours: list[datetime] = []
    cur = start_dt
    while cur < end_dt:
        hours.append(cur)
        cur += timedelta(hours=1)
    return hours


def _s3_key_for_hour(dt: datetime) -> tuple[str, str]:
    """Return (format_type, s3_key) for a given hour. Empty key if too old."""
    date_str = dt.strftime("%Y%m%d")
    h = dt.hour
    if dt >= _FILLS_BY_BLOCK_START:
        return "fills_by_block", f"node_fills_by_block/hourly/{date_str}/{h}.lz4"
    if dt >= _NODE_FILLS_START:
        return "node_fills", f"node_fills/hourly/{date_str}/{h}.lz4"
    if dt >= _NODE_TRADES_START:
        return "node_trades", f"node_trades/hourly/{date_str}/{h}.lz4"
    return "none", ""


# ---------------------------------------------------------------------------
# Parsers — extract (time_ms, price, size) for a single coin from raw NDJSON
# ---------------------------------------------------------------------------


def _parse_fills_by_block(
    raw: str, coin: str, start_ms: int, end_ms: int
) -> list[tuple[int, float, float]]:
    fills: list[tuple[int, float, float]] = []
    seen: set[int] = set()
    for line in raw.split("\n"):
        if not line:
            continue
        try:
            block = json.loads(line)
        except json.JSONDecodeError:
            continue
        for event in block.get("events", []):
            if not isinstance(event, list) or len(event) < 2:
                continue
            f = event[1]
            if f.get("coin") != coin:
                continue
            t = f.get("time", 0)
            if t < start_ms or t >= end_ms:
                continue
            tid = f.get("tid")
            if tid is not None and tid in seen:
                continue
            if tid is not None:
                seen.add(tid)
            fills.append((t, float(f["px"]), float(f["sz"])))
    return fills


def _parse_node_fills(
    raw: str, coin: str, start_ms: int, end_ms: int
) -> list[tuple[int, float, float]]:
    fills: list[tuple[int, float, float]] = []
    seen: set[int] = set()
    for line in raw.split("\n"):
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, list) or len(event) < 2:
            continue
        f = event[1]
        if f.get("coin") != coin:
            continue
        t = f.get("time", 0)
        if t < start_ms or t >= end_ms:
            continue
        tid = f.get("tid")
        if tid is not None and tid in seen:
            continue
        if tid is not None:
            seen.add(tid)
        fills.append((t, float(f["px"]), float(f["sz"])))
    return fills


def _parse_node_trades(
    raw: str, coin: str, start_ms: int, end_ms: int
) -> list[tuple[int, float, float]]:
    fills: list[tuple[int, float, float]] = []
    for line in raw.split("\n"):
        if not line:
            continue
        try:
            trade = json.loads(line)
        except json.JSONDecodeError:
            continue
        if trade.get("coin") != coin:
            continue
        time_str = trade.get("time", "")
        try:
            dt = datetime.fromisoformat(time_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            t = int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            continue
        if t < start_ms or t >= end_ms:
            continue
        fills.append((t, float(trade["px"]), float(trade["sz"])))
    return fills


_PARSERS = {
    "fills_by_block": _parse_fills_by_block,
    "node_fills": _parse_node_fills,
    "node_trades": _parse_node_trades,
}


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def _aggregate_to_candles(
    fills: list[tuple[int, float, float]],
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list[Candle]:
    if not fills:
        return []

    interval_ms = INTERVAL_SECONDS[interval] * 1000
    aligned_start = (start_ms // interval_ms) * interval_ms
    buckets: dict[int, list[tuple[int, float, float]]] = defaultdict(list)

    for time_ms, price, size in fills:
        bucket = (time_ms // interval_ms) * interval_ms
        if bucket < aligned_start or bucket >= end_ms:
            continue
        buckets[bucket].append((time_ms, price, size))

    candles: list[Candle] = []
    for bucket_ms in sorted(buckets):
        bucket_fills = sorted(buckets[bucket_ms])  # sort by time
        prices = [p for _, p, _ in bucket_fills]
        volumes = [s for _, _, s in bucket_fills]
        candles.append(
            Candle(
                timestamp_ms=bucket_ms,
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=sum(volumes),
            )
        )
    return candles


# ---------------------------------------------------------------------------
# Download + decompress a single hourly file
# ---------------------------------------------------------------------------


def _try_decompress(data: bytes) -> str:
    """Try LZ4 frame decompression; fall back to plain UTF-8."""
    try:
        import lz4.frame

        return lz4.frame.decompress(data).decode("utf-8")
    except Exception:
        return data.decode("utf-8")


def _download_hour(s3: "S3Client", s3_key: str) -> bytes:
    resp = s3.get_object(Bucket=S3_BUCKET, Key=s3_key, RequestPayer="requester")
    return resp["Body"].read()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@op()
async def fetch_s3_candles(
    asset: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
) -> list[Candle]:
    """Download fills from HL S3 and aggregate into OHLCV candles."""
    if interval not in INTERVAL_SECONDS:
        logger.debug(f"S3: unsupported interval {interval}")
        return []

    try:
        s3 = _get_s3_client()
    except Exception as e:
        logger.debug(f"S3 client init failed (missing credentials?): {e}")
        return []

    hours = _hours_in_range(start_ms, end_ms)
    if not hours:
        return []

    # Build download tasks: (format_type, s3_key, hour_dt)
    tasks: list[tuple[str, str, datetime]] = []
    for dt in hours:
        fmt, key = _s3_key_for_hour(dt)
        if fmt == "none":
            continue
        tasks.append((fmt, key, dt))

    if not tasks:
        logger.debug(f"S3: no data available for {asset} in requested range")
        return []

    sem = asyncio.Semaphore(_MAX_CONCURRENT_DOWNLOADS)

    async def _download_one(s3_key: str) -> bytes | None:
        async with sem:
            try:
                return await asyncio.to_thread(_download_hour, s3, s3_key)
            except Exception as e:
                logger.debug(f"S3 download failed {s3_key}: {e}")
                return None

    # Download all hours concurrently
    download_results = await asyncio.gather(
        *[_download_one(key) for _, key, _ in tasks]
    )

    # Parse fills from downloaded data
    all_fills: list[tuple[int, float, float]] = []
    for (fmt, key, _dt), raw_bytes in zip(tasks, download_results):
        if raw_bytes is None:
            continue
        raw_text = _try_decompress(raw_bytes)
        parser = _PARSERS[fmt]
        fills = parser(raw_text, asset, start_ms, end_ms)
        all_fills.extend(fills)

    if not all_fills:
        logger.debug(f"S3: no fills found for {asset} in {len(tasks)} hourly files")
        return []

    candles = _aggregate_to_candles(all_fills, interval, start_ms, end_ms)
    logger.debug(
        f"S3: {asset}/{interval} → {len(candles)} candles from {len(all_fills)} fills"
    )
    return candles
