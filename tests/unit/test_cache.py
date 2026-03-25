import shutil
from pathlib import Path

import pytest

from agent_trader.data.cache import CACHE_DIR, load_cached_candles, save_to_cache
from agent_trader.data.market import Candle

TEST_CACHE = CACHE_DIR / "_test_asset"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if TEST_CACHE.exists():
        shutil.rmtree(TEST_CACHE)


def _make_candles(start_ms, count, interval_ms=60_000):
    return [
        Candle(
            timestamp_ms=start_ms + i * interval_ms,
            open=100.0 + i, high=101.0 + i, low=99.0 + i,
            close=100.5 + i, volume=10.0,
        )
        for i in range(count)
    ]


def test_empty_cache_returns_none():
    result = load_cached_candles("_test_asset", "1m", 0, 1000000)
    assert result is None


def test_save_and_load():
    candles = _make_candles(1000000, 5)
    save_to_cache("_test_asset", "1m", candles)

    loaded = load_cached_candles("_test_asset", "1m", 1000000, 1000000 + 5 * 60_000)
    assert loaded is not None
    assert len(loaded) == 5
    assert loaded[0].timestamp_ms == 1000000
    assert loaded[0].close == 100.5


def test_load_filters_by_range():
    candles = _make_candles(1000000, 10)
    save_to_cache("_test_asset", "1m", candles)

    loaded = load_cached_candles("_test_asset", "1m", 1000000 + 3 * 60_000, 1000000 + 7 * 60_000)
    assert loaded is not None
    assert len(loaded) == 4


def test_merge_deduplicates():
    candles1 = _make_candles(1000000, 5)
    candles2 = _make_candles(1000000 + 3 * 60_000, 5)
    save_to_cache("_test_asset", "1m", candles1)
    save_to_cache("_test_asset", "1m", candles2)

    loaded = load_cached_candles("_test_asset", "1m", 1000000, 1000000 + 10 * 60_000)
    assert loaded is not None
    assert len(loaded) == 8
    timestamps = [c.timestamp_ms for c in loaded]
    assert timestamps == sorted(set(timestamps))


def test_builder_dex_asset_path():
    candles = _make_candles(1000000, 3)
    save_to_cache("xyz:GOLD", "1h", candles)

    path = CACHE_DIR / "xyz_GOLD" / "1h.parquet"
    assert path.exists()

    loaded = load_cached_candles("xyz:GOLD", "1h", 1000000, 1000000 + 3 * 60_000)
    assert loaded is not None
    assert len(loaded) == 3

    shutil.rmtree(CACHE_DIR / "xyz_GOLD")
