from unittest.mock import AsyncMock, patch

import pytest

from agent_trader.data.market import Candle, fetch_candles


def _fake_candles(n=3):
    return [
        Candle(timestamp_ms=1000000 + i * 60_000, open=100.0, high=101.0, low=99.0, close=100.5, volume=10.0)
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_cache_hit_skips_network():
    with patch("agent_trader.data.cache.load_cached_candles", return_value=_fake_candles()):
        candles = await fetch_candles("BTC", 1000000, 1200000, "1m")
        assert len(candles) == 3


@pytest.mark.asyncio
async def test_bybit_used_when_cache_miss():
    with (
        patch("agent_trader.data.cache.load_cached_candles", return_value=None),
        patch("agent_trader.data.bybit.fetch_bybit_candles", new_callable=AsyncMock, return_value=_fake_candles()),
        patch("agent_trader.data.cache.save_to_cache"),
    ):
        candles = await fetch_candles("BTC", 1000000, 1200000, "1m")
        assert len(candles) == 3


@pytest.mark.asyncio
async def test_hl_used_when_bybit_empty():
    with (
        patch("agent_trader.data.cache.load_cached_candles", return_value=None),
        patch("agent_trader.data.bybit.fetch_bybit_candles", new_callable=AsyncMock, return_value=[]),
        patch("agent_trader.data.market._fetch_hl_candles", new_callable=AsyncMock, return_value=_fake_candles(2)),
        patch("agent_trader.data.cache.save_to_cache"),
    ):
        candles = await fetch_candles("BTC", 1000000, 1200000, "1m")
        assert len(candles) == 2


@pytest.mark.asyncio
async def test_returns_empty_when_all_sources_fail():
    with (
        patch("agent_trader.data.cache.load_cached_candles", return_value=None),
        patch("agent_trader.data.bybit.fetch_bybit_candles", new_callable=AsyncMock, return_value=[]),
        patch("agent_trader.data.market._fetch_hl_candles", new_callable=AsyncMock, return_value=[]),
        patch("agent_trader.data.s3_fills.fetch_s3_candles", new_callable=AsyncMock, return_value=[]),
    ):
        candles = await fetch_candles("kDOGS", 1000000, 1200000, "1m")
        assert candles == []
