import httpx
from loguru import logger
from pydantic import BaseModel

from agent_trader.constants import CHART_WINDOW, TIMEFRAME_MS

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
MAX_CANDLES_PER_REQUEST = 500


class Candle(BaseModel):
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


async def _fetch_hl_candles(
    asset: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
    client: httpx.AsyncClient | None = None,
) -> list[Candle]:
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=30)

    all_candles: list[Candle] = []
    cursor = start_ms

    try:
        while cursor < end_ms:
            payload = {
                "type": "candleSnapshot",
                "req": {
                    "coin": asset,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                },
            }
            resp = await client.post(HL_INFO_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                break

            for c in data:
                all_candles.append(
                    Candle(
                        timestamp_ms=c["t"],
                        open=float(c["o"]),
                        high=float(c["h"]),
                        low=float(c["l"]),
                        close=float(c["c"]),
                        volume=float(c["v"]),
                    )
                )

            if len(data) < MAX_CANDLES_PER_REQUEST:
                break

            cursor = data[-1]["t"] + 1
    finally:
        if own_client:
            await client.aclose()

    return all_candles


async def fetch_candles(
    asset: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
    client: httpx.AsyncClient | None = None,
) -> list[Candle]:
    from agent_trader.data.bybit import fetch_bybit_candles
    from agent_trader.data.cache import load_cached_candles, save_to_cache

    cached = load_cached_candles(asset, interval, start_ms, end_ms)
    if cached:
        logger.debug(f"Cache hit: {asset}/{interval} ({len(cached)} candles)")
        return cached

    candles = await fetch_bybit_candles(asset, start_ms, end_ms, interval, client)
    if candles:
        logger.debug(f"Bybit: {asset}/{interval} → {len(candles)} candles")
        save_to_cache(asset, interval, candles)
        return candles

    try:
        candles = await _fetch_hl_candles(asset, start_ms, end_ms, interval, client)
    except Exception as e:
        logger.debug(f"HL API error for {asset}/{interval}: {e}")
        candles = []
    if candles:
        logger.debug(f"HL API: {asset}/{interval} → {len(candles)} candles")
        save_to_cache(asset, interval, candles)
        return candles

    from agent_trader.data.s3_fills import fetch_s3_candles

    candles = await fetch_s3_candles(asset, start_ms, end_ms, interval)
    if candles:
        logger.debug(f"S3 fills: {asset}/{interval} → {len(candles)} candles")
        save_to_cache(asset, interval, candles)
        return candles

    logger.warning(f"No candle data: {asset}/{interval} [{start_ms}..{end_ms}]")
    return []


async def fetch_price_change(
    asset: str,
    from_ms: int,
    timeframe: str,
    client: httpx.AsyncClient | None = None,
) -> tuple[float, float, float]:
    to_ms = from_ms + TIMEFRAME_MS[timeframe]

    candles_at = await fetch_candles(asset, from_ms, from_ms + 60_000, "1m", client)
    if not candles_at:
        raise ValueError(f"No candle data for {asset} at {from_ms}")
    price_at_post = candles_at[0].close

    candles_after = await fetch_candles(asset, to_ms, to_ms + 60_000, "1m", client)
    if not candles_after:
        raise ValueError(f"No candle data for {asset} at {to_ms}")
    price_after = candles_after[0].close

    change_pct = (price_after - price_at_post) / price_at_post * 100
    return price_at_post, price_after, change_pct


async def fetch_candles_for_chart(
    asset: str,
    post_ms: int,
    timeframe: str,
    client: httpx.AsyncClient | None = None,
) -> list[Candle]:
    window = CHART_WINDOW[timeframe]
    start_ms = post_ms - window["pad_ms"]
    end_ms = post_ms + window["pad_ms"]
    return await fetch_candles(asset, start_ms, end_ms, window["interval"], client)
