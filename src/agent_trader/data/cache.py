from pathlib import Path

import pandas as pd
from loguru import logger

from agent_trader.data.market import Candle

CACHE_DIR = Path("data/cache/candles")

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
    "4h": 14_400_000, "1d": 86_400_000,
}


def _cache_path(asset: str, interval: str) -> Path:
    safe_asset = asset.replace(":", "_")
    return CACHE_DIR / safe_asset / f"{interval}.parquet"


def load_cached_candles(
    asset: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list[Candle] | None:
    path = _cache_path(asset, interval)
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df = df[(df["timestamp_ms"] >= start_ms) & (df["timestamp_ms"] < end_ms)]

    if df.empty:
        return None

    step = INTERVAL_MS.get(interval)
    if step:
        expected = (end_ms - start_ms) // step
        if expected > 0 and len(df) < expected * 0.8:
            return None

    return [
        Candle(
            timestamp_ms=int(r.timestamp_ms),
            open=r.open,
            high=r.high,
            low=r.low,
            close=r.close,
            volume=r.volume,
        )
        for r in df.itertuples()
    ]


def save_to_cache(asset: str, interval: str, candles: list[Candle]) -> None:
    if not candles:
        return

    path = _cache_path(asset, interval)
    path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame([c.model_dump() for c in candles])

    if path.exists():
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["timestamp_ms"]).sort_values("timestamp_ms")
    else:
        merged = new_df.sort_values("timestamp_ms")

    merged.to_parquet(path, index=False)
    logger.debug(f"Cached {len(candles)} candles for {asset}/{interval}")
