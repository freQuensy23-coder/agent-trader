import httpx
from loguru import logger
from weave import op

from agent_trader.data.market import Candle

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
MAX_BYBIT_CANDLES = 200

INTERVAL_MAP = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "1d": "D",
}

_RENAMES = {
    "kPEPE": "1000PEPE",
    "kBONK": "1000BONK",
    "kFLOKI": "1000FLOKI",
    "kSHIB": "SHIB1000",
    "kLUNC": "1000LUNC",
    "kNEIRO": "1000NEIROCTO",
    "RNDR": "RENDER",
    "FTM": "SONIC",
    "MATIC": "POL",
    "TURBO": "1000TURBO",
    "JELLY": "JELLYJELLY",
    "HPOS": "HPOS10I",
    "TST": "TSTBSC",
}


def hl_to_bybit_symbol(hl_asset: str) -> str | None:
    if ":" in hl_asset:
        return None
    base = _RENAMES.get(hl_asset, hl_asset)
    return f"{base}USDT"


@op()
async def fetch_bybit_candles(
    hl_asset: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
    client: httpx.AsyncClient | None = None,
) -> list[Candle]:
    symbol = hl_to_bybit_symbol(hl_asset)
    if symbol is None:
        return []

    bb_interval = INTERVAL_MAP.get(interval)
    if bb_interval is None:
        return []

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=30)

    all_candles: list[Candle] = []
    cursor_end = end_ms

    try:
        while cursor_end > start_ms:
            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": bb_interval,
                "start": start_ms,
                "end": cursor_end,
                "limit": MAX_BYBIT_CANDLES,
            }
            resp = await client.get(BYBIT_KLINE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            if data.get("retCode") != 0:
                logger.debug(f"Bybit error for {symbol}: {data.get('retMsg')}")
                return []

            result_list = data.get("result", {}).get("list", [])
            if not result_list:
                break

            for item in result_list:
                ts = int(item[0])
                if ts < start_ms or ts >= end_ms:
                    continue
                all_candles.append(
                    Candle(
                        timestamp_ms=ts,
                        open=float(item[1]),
                        high=float(item[2]),
                        low=float(item[3]),
                        close=float(item[4]),
                        volume=float(item[5]),
                    )
                )

            if len(result_list) < MAX_BYBIT_CANDLES:
                break

            cursor_end = int(result_list[-1][0]) - 1
    except (httpx.HTTPStatusError, httpx.RequestError, KeyError, IndexError) as e:
        logger.debug(f"Bybit fetch failed for {hl_asset}: {e}")
        return []
    finally:
        if own_client:
            await client.aclose()

    return all_candles
