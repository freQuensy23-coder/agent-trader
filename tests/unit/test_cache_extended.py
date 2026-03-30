"""Tests for candle aggregation, funding cache, LS ratio cache, nearest candle."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from agent_trader.data.cache import (
    INTERVAL_MS,
    aggregate_candles_from_1m,
    load_cached_candles_via_aggregation,
    load_latest_funding,
    load_latest_ls_ratio,
    load_nearest_cached_candle,
    save_funding_to_cache,
    save_ls_ratio_to_cache,
    save_to_cache,
)
from agent_trader.data.market import Candle


def _make_1m_candles(start_ms: int, count: int, base_price: float = 100.0) -> list[Candle]:
    candles = []
    for i in range(count):
        p = base_price + i * 0.1
        candles.append(Candle(
            timestamp_ms=start_ms + i * 60_000,
            open=p, high=p + 0.5, low=p - 0.3, close=p + 0.2, volume=10.0 + i,
        ))
    return candles


class TestAggregateCandles:
    def test_1m_to_5m(self):
        candles = _make_1m_candles(0, 10)
        result = aggregate_candles_from_1m(candles, "5m")
        assert len(result) == 2
        assert result[0].timestamp_ms == 0
        assert result[1].timestamp_ms == 300_000
        assert result[0].open == candles[0].open
        assert result[0].close == candles[4].close
        assert result[0].high == max(c.high for c in candles[:5])
        assert result[0].low == min(c.low for c in candles[:5])
        assert result[0].volume == sum(c.volume for c in candles[:5])

    def test_1m_to_1h(self):
        candles = _make_1m_candles(0, 60)
        result = aggregate_candles_from_1m(candles, "1h")
        assert len(result) == 1
        assert result[0].open == candles[0].open
        assert result[0].close == candles[59].close

    def test_1m_to_1d(self):
        candles = _make_1m_candles(0, 1440)
        result = aggregate_candles_from_1m(candles, "1d")
        assert len(result) == 1
        assert result[0].volume == sum(c.volume for c in candles)

    def test_1m_passthrough(self):
        candles = _make_1m_candles(0, 5)
        result = aggregate_candles_from_1m(candles, "1m")
        assert result is candles

    def test_empty_input(self):
        assert aggregate_candles_from_1m([], "5m") == []


class TestLoadViaAggregation:
    @patch("agent_trader.data.cache.load_cached_candles")
    def test_returns_aggregated(self, mock_load):
        candles = _make_1m_candles(0, 60)
        mock_load.return_value = candles
        result = load_cached_candles_via_aggregation("BTC", "5m", 0, 3_600_000)
        assert result is not None
        assert len(result) == 12
        mock_load.assert_called_once_with("BTC", "1m", 0, 3_600_000)

    @patch("agent_trader.data.cache.load_cached_candles")
    def test_returns_none_for_1m(self, mock_load):
        result = load_cached_candles_via_aggregation("BTC", "1m", 0, 60_000)
        assert result is None
        mock_load.assert_not_called()

    @patch("agent_trader.data.cache.load_cached_candles")
    def test_returns_none_when_no_1m_cache(self, mock_load):
        mock_load.return_value = None
        result = load_cached_candles_via_aggregation("BTC", "5m", 0, 3_600_000)
        assert result is None

    @patch("agent_trader.data.cache.load_cached_candles")
    def test_returns_none_when_incomplete(self, mock_load):
        candles = _make_1m_candles(0, 10)  # only 10 out of 60 needed
        mock_load.return_value = candles
        result = load_cached_candles_via_aggregation("BTC", "5m", 0, 3_600_000)
        assert result is None  # 2 candles < 12 * 0.8


class TestNearestCandle:
    def test_finds_nearest(self, tmp_path):
        candles = _make_1m_candles(1000 * 60_000, 5)
        with patch("agent_trader.data.cache._cache_path", return_value=tmp_path / "1m.parquet"):
            df = pd.DataFrame([c.model_dump() for c in candles])
            (tmp_path / "1m.parquet").parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(tmp_path / "1m.parquet", index=False)

            result = load_nearest_cached_candle("BTC", 1005 * 60_000)
            assert result is not None
            assert result.timestamp_ms == 1004 * 60_000

    def test_returns_none_when_empty(self, tmp_path):
        with patch("agent_trader.data.cache._cache_path", return_value=tmp_path / "1m.parquet"):
            assert load_nearest_cached_candle("BTC", 1000) is None


class TestFundingCache:
    def test_round_trip(self, tmp_path):
        entries = [(1000, 0.0001), (2000, 0.0002), (3000, -0.0001)]
        with patch("agent_trader.data.cache._funding_cache_path", return_value=tmp_path / "BTC.parquet"):
            save_funding_to_cache("BTC", entries)
            result = load_latest_funding("BTC", 2500)
            assert result == pytest.approx(0.0002)

    def test_latest_before(self, tmp_path):
        entries = [(1000, 0.01), (5000, 0.02), (9000, 0.03)]
        with patch("agent_trader.data.cache._funding_cache_path", return_value=tmp_path / "X.parquet"):
            save_funding_to_cache("X", entries)
            assert load_latest_funding("X", 6000) == pytest.approx(0.02)
            assert load_latest_funding("X", 1000) is None  # nothing before 1000

    def test_none_when_missing(self, tmp_path):
        with patch("agent_trader.data.cache._funding_cache_path", return_value=tmp_path / "NONE.parquet"):
            assert load_latest_funding("NONE", 1000) is None


class TestLSRatioCache:
    def test_round_trip(self, tmp_path):
        entries = [(1000, 0.55, 0.45), (2000, 0.60, 0.40)]
        with patch("agent_trader.data.cache._ls_ratio_cache_path", return_value=tmp_path / "BTC.parquet"):
            save_ls_ratio_to_cache("BTC", entries)
            result = load_latest_ls_ratio("BTC", 2500)
            assert result is not None
            assert result[0] == pytest.approx(0.60)
            assert result[1] == pytest.approx(0.40)

    def test_none_when_missing(self, tmp_path):
        with patch("agent_trader.data.cache._ls_ratio_cache_path", return_value=tmp_path / "NONE.parquet"):
            assert load_latest_ls_ratio("NONE", 1000) is None
