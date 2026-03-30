from collections import defaultdict
from pathlib import Path

import pandas as pd
from loguru import logger

from agent_trader.data.market import Candle

CACHE_DIR = Path("data/cache/candles")
FUNDING_CACHE_DIR = Path("data/cache/funding")
LS_RATIO_CACHE_DIR = Path("data/cache/ls_ratio")

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000,
    "4h": 14_400_000, "1d": 86_400_000,
}


def _cache_path(asset: str, interval: str) -> Path:
    safe_asset = asset.replace(":", "_")
    return CACHE_DIR / safe_asset / f"{interval}.parquet"


# ---------------------------------------------------------------------------
# Candle cache
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Candle aggregation from 1m
# ---------------------------------------------------------------------------


def aggregate_candles_from_1m(
    candles_1m: list[Candle],
    target_interval: str,
) -> list[Candle]:
    target_ms = INTERVAL_MS.get(target_interval)
    if target_ms is None or target_ms <= 60_000:
        return candles_1m

    buckets: dict[int, list[Candle]] = defaultdict(list)
    for c in candles_1m:
        bucket = (c.timestamp_ms // target_ms) * target_ms
        buckets[bucket].append(c)

    result = []
    for bucket_ms in sorted(buckets):
        group = buckets[bucket_ms]
        result.append(Candle(
            timestamp_ms=bucket_ms,
            open=group[0].open,
            high=max(c.high for c in group),
            low=min(c.low for c in group),
            close=group[-1].close,
            volume=sum(c.volume for c in group),
        ))
    return result


def load_cached_candles_via_aggregation(
    asset: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list[Candle] | None:
    if interval == "1m":
        return None

    candles_1m = load_cached_candles(asset, "1m", start_ms, end_ms)
    if not candles_1m:
        return None

    aggregated = aggregate_candles_from_1m(candles_1m, interval)
    if not aggregated:
        return None

    target_ms = INTERVAL_MS.get(interval)
    if target_ms:
        expected = (end_ms - start_ms) // target_ms
        if expected > 0 and len(aggregated) < expected * 0.8:
            return None

    return aggregated


# ---------------------------------------------------------------------------
# Nearest candle lookup (for sparse builder DEX assets)
# ---------------------------------------------------------------------------


def load_nearest_cached_candle(asset: str, before_ms: int) -> Candle | None:
    path = _cache_path(asset, "1m")
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df = df[df["timestamp_ms"] < before_ms]
    if df.empty:
        return None

    row = df.loc[df["timestamp_ms"].idxmax()]
    return Candle(
        timestamp_ms=int(row.timestamp_ms),
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
    )


# ---------------------------------------------------------------------------
# Funding cache
# ---------------------------------------------------------------------------


def _funding_cache_path(asset: str) -> Path:
    safe_asset = asset.replace(":", "_")
    return FUNDING_CACHE_DIR / f"{safe_asset}.parquet"


def load_cached_funding(
    asset: str,
    start_ms: int,
    end_ms: int,
) -> list[tuple[int, float]] | None:
    path = _funding_cache_path(asset)
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df = df[(df["timestamp_ms"] >= start_ms) & (df["timestamp_ms"] < end_ms)]
    if df.empty:
        return None

    return [(int(r.timestamp_ms), float(r.funding_rate)) for r in df.itertuples()]


def load_latest_funding(asset: str, before_ms: int) -> float | None:
    path = _funding_cache_path(asset)
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df = df[df["timestamp_ms"] < before_ms]
    if df.empty:
        return None

    return float(df.loc[df["timestamp_ms"].idxmax(), "funding_rate"])


def save_funding_to_cache(asset: str, entries: list[tuple[int, float]]) -> None:
    if not entries:
        return

    path = _funding_cache_path(asset)
    path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(entries, columns=["timestamp_ms", "funding_rate"])

    if path.exists():
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["timestamp_ms"]).sort_values("timestamp_ms")
    else:
        merged = new_df.sort_values("timestamp_ms")

    merged.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Long/short ratio cache
# ---------------------------------------------------------------------------


def _ls_ratio_cache_path(asset: str) -> Path:
    safe_asset = asset.replace(":", "_")
    return LS_RATIO_CACHE_DIR / f"{safe_asset}.parquet"


def load_cached_ls_ratio(
    asset: str,
    start_ms: int,
    end_ms: int,
) -> list[tuple[int, float, float]] | None:
    path = _ls_ratio_cache_path(asset)
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df = df[(df["timestamp_ms"] >= start_ms) & (df["timestamp_ms"] < end_ms)]
    if df.empty:
        return None

    return [(int(r.timestamp_ms), float(r.buy_ratio), float(r.sell_ratio)) for r in df.itertuples()]


def load_latest_ls_ratio(asset: str, before_ms: int) -> tuple[float, float] | None:
    path = _ls_ratio_cache_path(asset)
    if not path.exists():
        return None

    df = pd.read_parquet(path)
    df = df[df["timestamp_ms"] < before_ms]
    if df.empty:
        return None

    row = df.loc[df["timestamp_ms"].idxmax()]
    return (float(row.buy_ratio), float(row.sell_ratio))


def save_ls_ratio_to_cache(asset: str, entries: list[tuple[int, float, float]]) -> None:
    if not entries:
        return

    path = _ls_ratio_cache_path(asset)
    path.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame(entries, columns=["timestamp_ms", "buy_ratio", "sell_ratio"])

    if path.exists():
        existing = pd.read_parquet(path)
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["timestamp_ms"]).sort_values("timestamp_ms")
    else:
        merged = new_df.sort_values("timestamp_ms")

    merged.to_parquet(path, index=False)
