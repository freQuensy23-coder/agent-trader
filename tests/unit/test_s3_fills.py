import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from agent_trader.data.market import Candle
from agent_trader.data.s3_fills import (
    _aggregate_to_candles,
    _hours_in_range,
    _parse_fills_by_block,
    _parse_node_fills,
    _parse_node_trades,
    _s3_key_for_hour,
    _try_decompress,
    fetch_s3_candles,
)


# ---------------------------------------------------------------------------
# _hours_in_range
# ---------------------------------------------------------------------------


def test_hours_single():
    # 30-minute window within one hour
    start = int(datetime(2025, 8, 1, 10, 15, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2025, 8, 1, 10, 45, tzinfo=timezone.utc).timestamp() * 1000)
    hours = _hours_in_range(start, end)
    assert len(hours) == 1
    assert hours[0].hour == 10


def test_hours_span_three():
    start = int(datetime(2025, 8, 1, 10, 30, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2025, 8, 1, 12, 30, tzinfo=timezone.utc).timestamp() * 1000)
    hours = _hours_in_range(start, end)
    assert len(hours) == 3
    assert [h.hour for h in hours] == [10, 11, 12]


# ---------------------------------------------------------------------------
# _s3_key_for_hour
# ---------------------------------------------------------------------------


def test_key_fills_by_block():
    dt = datetime(2025, 8, 15, 14, tzinfo=timezone.utc)
    fmt, key = _s3_key_for_hour(dt)
    assert fmt == "fills_by_block"
    assert key == "node_fills_by_block/hourly/20250815/14"


def test_key_node_fills():
    dt = datetime(2025, 6, 10, 3, tzinfo=timezone.utc)
    fmt, key = _s3_key_for_hour(dt)
    assert fmt == "node_fills"
    assert key == "node_fills/hourly/20250610/3"


def test_key_node_trades():
    dt = datetime(2025, 4, 1, 0, tzinfo=timezone.utc)
    fmt, key = _s3_key_for_hour(dt)
    assert fmt == "node_trades"
    assert key == "node_trades/hourly/20250401/0"


def test_key_too_old():
    dt = datetime(2025, 1, 1, 0, tzinfo=timezone.utc)
    fmt, key = _s3_key_for_hour(dt)
    assert fmt == "none"
    assert key == ""


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

# Helpers to build test data

def _make_fills_by_block_line(events: list[dict], block_time_ms: int = 0) -> str:
    block = {
        "local_time": "2025-08-01T10:00:00",
        "block_time": "2025-08-01T10:00:00",
        "block_number": 700000000,
        "events": [["0xuser", e] for e in events],
    }
    return json.dumps(block)


def _make_fill(coin: str, px: str, sz: str, time_ms: int, tid: int, side: str = "B") -> dict:
    return {
        "coin": coin,
        "px": px,
        "sz": sz,
        "side": side,
        "time": time_ms,
        "tid": tid,
        "startPosition": "0",
        "dir": "Open Long",
        "closedPnl": "0",
        "hash": "0x0",
        "oid": 1,
        "crossed": False,
        "fee": "0.01",
        "feeToken": "USDC",
    }


class TestParseFillsByBlock:
    def test_basic(self):
        fills = [
            _make_fill("BTC", "100000.0", "0.5", 1000, 1),
            _make_fill("BTC", "100100.0", "0.3", 2000, 2),
            _make_fill("ETH", "3500.0", "1.0", 1500, 3),  # different coin
        ]
        raw = _make_fills_by_block_line(fills)
        result = _parse_fills_by_block(raw, "BTC", 0, 10000)
        assert len(result) == 2
        assert result[0] == (1000, 100000.0, 0.5)
        assert result[1] == (2000, 100100.0, 0.3)

    def test_dedup_by_tid(self):
        fill = _make_fill("BTC", "100000.0", "0.5", 1000, 1)
        raw = _make_fills_by_block_line([fill]) + "\n" + _make_fills_by_block_line([fill])
        result = _parse_fills_by_block(raw, "BTC", 0, 10000)
        assert len(result) == 1

    def test_time_filter(self):
        fills = [
            _make_fill("BTC", "100000.0", "0.5", 500, 1),
            _make_fill("BTC", "100100.0", "0.3", 1500, 2),
            _make_fill("BTC", "100200.0", "0.2", 2500, 3),
        ]
        raw = _make_fills_by_block_line(fills)
        result = _parse_fills_by_block(raw, "BTC", 1000, 2000)
        assert len(result) == 1
        assert result[0][0] == 1500

    def test_empty_data(self):
        assert _parse_fills_by_block("", "BTC", 0, 10000) == []

    def test_malformed_json_skipped(self):
        raw = "not json\n" + _make_fills_by_block_line(
            [_make_fill("BTC", "100000.0", "0.5", 1000, 1)]
        )
        result = _parse_fills_by_block(raw, "BTC", 0, 10000)
        assert len(result) == 1


class TestParseNodeFills:
    def test_basic(self):
        fill = _make_fill("BTC", "99000.0", "1.0", 5000, 10)
        raw = json.dumps(["0xuser", fill])
        result = _parse_node_fills(raw, "BTC", 0, 10000)
        assert len(result) == 1
        assert result[0] == (5000, 99000.0, 1.0)

    def test_dedup(self):
        fill = _make_fill("BTC", "99000.0", "1.0", 5000, 10)
        raw = json.dumps(["0xuser", fill]) + "\n" + json.dumps(["0xuser", fill])
        result = _parse_node_fills(raw, "BTC", 0, 10000)
        assert len(result) == 1

    def test_wrong_coin_filtered(self):
        fill = _make_fill("ETH", "3500.0", "2.0", 5000, 10)
        raw = json.dumps(["0xuser", fill])
        result = _parse_node_fills(raw, "BTC", 0, 10000)
        assert result == []


class TestParseNodeTrades:
    def test_basic(self):
        trade = {
            "coin": "BTC",
            "px": "65000.0",
            "sz": "0.1",
            "side": "B",
            "time": "2025-04-15T12:30:45.123456",
            "hash": "0x0",
            "trade_dir_override": "Na",
            "side_info": [],
        }
        raw = json.dumps(trade)
        # April 15 2025 12:30 is within node_trades range
        result = _parse_node_trades(raw, "BTC", 0, int(2e15))
        assert len(result) == 1
        assert result[0][1] == 65000.0

    def test_iso_time_parsed(self):
        trade = {
            "coin": "SOL",
            "px": "150.0",
            "sz": "10",
            "side": "A",
            "time": "2025-04-01T00:00:00",
            "hash": "0x0",
        }
        raw = json.dumps(trade)
        expected_ms = int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp() * 1000)
        result = _parse_node_trades(raw, "SOL", expected_ms - 1000, expected_ms + 1000)
        assert len(result) == 1
        assert result[0][0] == expected_ms

    def test_wrong_coin(self):
        trade = {"coin": "ETH", "px": "3000.0", "sz": "1", "side": "B", "time": "2025-04-01T00:00:00"}
        raw = json.dumps(trade)
        assert _parse_node_trades(raw, "BTC", 0, int(2e15)) == []


# ---------------------------------------------------------------------------
# _aggregate_to_candles
# ---------------------------------------------------------------------------


class TestAggregate:
    def test_single_fill(self):
        fills = [(60000, 100.0, 1.0)]  # at 60s
        candles = _aggregate_to_candles(fills, "1m", 0, 120000)
        assert len(candles) == 1
        assert candles[0].timestamp_ms == 60000
        assert candles[0].open == 100.0
        assert candles[0].close == 100.0
        assert candles[0].volume == 1.0

    def test_multiple_fills_one_candle(self):
        fills = [
            (60000, 100.0, 1.0),
            (65000, 105.0, 2.0),
            (70000, 98.0, 0.5),
            (80000, 102.0, 1.5),
        ]
        candles = _aggregate_to_candles(fills, "1m", 0, 120000)
        assert len(candles) == 1
        c = candles[0]
        assert c.timestamp_ms == 60000
        assert c.open == 100.0
        assert c.high == 105.0
        assert c.low == 98.0
        assert c.close == 102.0
        assert c.volume == 5.0

    def test_two_candles(self):
        fills = [
            (60000, 100.0, 1.0),   # minute 1
            (120000, 200.0, 2.0),  # minute 2
        ]
        candles = _aggregate_to_candles(fills, "1m", 0, 180000)
        assert len(candles) == 2
        assert candles[0].timestamp_ms == 60000
        assert candles[1].timestamp_ms == 120000

    def test_empty_fills(self):
        assert _aggregate_to_candles([], "1m", 0, 60000) == []

    def test_5m_interval(self):
        fills = [
            (300000, 100.0, 1.0),   # 5:00
            (350000, 101.0, 1.0),   # 5:50
            (600000, 200.0, 2.0),   # 10:00
        ]
        candles = _aggregate_to_candles(fills, "5m", 0, 900000)
        assert len(candles) == 2
        assert candles[0].timestamp_ms == 300000
        assert candles[1].timestamp_ms == 600000


# ---------------------------------------------------------------------------
# _try_decompress
# ---------------------------------------------------------------------------


def test_decompress_plain_text():
    data = b'{"coin": "BTC"}'
    assert _try_decompress(data) == '{"coin": "BTC"}'


def test_decompress_lz4():
    import lz4.frame

    original = b'{"coin": "BTC", "px": "100000"}'
    compressed = lz4.frame.compress(original)
    assert _try_decompress(compressed) == original.decode("utf-8")


# ---------------------------------------------------------------------------
# fetch_s3_candles (mocked S3)
# ---------------------------------------------------------------------------


def _mock_s3_response(body_bytes: bytes) -> dict:
    body = MagicMock()
    body.read.return_value = body_bytes
    return {"Body": body}


@pytest.mark.asyncio
async def test_fetch_s3_candles_basic():
    """End-to-end test with mocked S3 client."""
    # Build fills for BTC on Aug 1 2025, 10:00-10:59 UTC
    base_ms = int(datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    fills = [
        _make_fill("BTC", "100000.0", "0.5", base_ms + 1000, 1),
        _make_fill("BTC", "100500.0", "0.3", base_ms + 30000, 2),
        _make_fill("BTC", "99500.0", "0.2", base_ms + 55000, 3),
        _make_fill("ETH", "3500.0", "1.0", base_ms + 2000, 4),  # ignored
    ]
    raw = _make_fills_by_block_line(fills)

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = _mock_s3_response(raw.encode("utf-8"))

    with patch("agent_trader.data.s3_fills._get_s3_client", return_value=mock_s3):
        candles = await fetch_s3_candles(
            "BTC", base_ms, base_ms + 60000, "1m"
        )

    assert len(candles) == 1
    assert candles[0].open == 100000.0
    assert candles[0].high == 100500.0
    assert candles[0].low == 99500.0
    assert candles[0].close == 99500.0
    assert candles[0].volume == pytest.approx(1.0)

    # Verify requester-pays was used
    call_kwargs = mock_s3.get_object.call_args.kwargs
    assert call_kwargs["RequestPayer"] == "requester"


@pytest.mark.asyncio
async def test_fetch_s3_candles_no_credentials():
    """Gracefully returns [] when boto3 not configured."""
    with patch("agent_trader.data.s3_fills._get_s3_client", side_effect=Exception("No creds")):
        candles = await fetch_s3_candles("BTC", 1000, 2000, "1m")
    assert candles == []


@pytest.mark.asyncio
async def test_fetch_s3_candles_download_failure():
    """Returns [] when S3 download fails."""
    base_ms = int(datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("Access Denied")

    with patch("agent_trader.data.s3_fills._get_s3_client", return_value=mock_s3):
        candles = await fetch_s3_candles("BTC", base_ms, base_ms + 60000, "1m")
    assert candles == []


@pytest.mark.asyncio
async def test_fetch_s3_no_fills_for_coin():
    """Returns [] when S3 data has no fills for requested coin."""
    base_ms = int(datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    fills = [_make_fill("ETH", "3500.0", "1.0", base_ms + 1000, 1)]
    raw = _make_fills_by_block_line(fills)

    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = _mock_s3_response(raw.encode("utf-8"))

    with patch("agent_trader.data.s3_fills._get_s3_client", return_value=mock_s3):
        candles = await fetch_s3_candles("BTC", base_ms, base_ms + 60000, "1m")
    assert candles == []


@pytest.mark.asyncio
async def test_fetch_s3_candles_too_old_range():
    """Returns [] for dates before node_trades start."""
    start = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2025, 1, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

    mock_s3 = MagicMock()
    with patch("agent_trader.data.s3_fills._get_s3_client", return_value=mock_s3):
        candles = await fetch_s3_candles("BTC", start, end, "1m")
    assert candles == []
    mock_s3.get_object.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_s3_multi_hour():
    """Downloads multiple hourly files and merges fills."""
    base_ms = int(datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    hour_ms = 3600 * 1000

    fill_h0 = _make_fill("BTC", "100000.0", "1.0", base_ms + 1000, 1)
    fill_h1 = _make_fill("BTC", "101000.0", "2.0", base_ms + hour_ms + 1000, 2)

    raw_h0 = _make_fills_by_block_line([fill_h0]).encode("utf-8")
    raw_h1 = _make_fills_by_block_line([fill_h1]).encode("utf-8")

    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = [
        _mock_s3_response(raw_h0),
        _mock_s3_response(raw_h1),
    ]

    with patch("agent_trader.data.s3_fills._get_s3_client", return_value=mock_s3):
        candles = await fetch_s3_candles(
            "BTC", base_ms, base_ms + 2 * hour_ms, "1h"
        )

    assert len(candles) == 2
    assert candles[0].close == 100000.0
    assert candles[1].close == 101000.0


# ---------------------------------------------------------------------------
# Integration with fetch_candles (level 4 fallback)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_market_falls_through_to_s3():
    """fetch_candles uses S3 when cache, Bybit, and HL all return empty."""
    from unittest.mock import AsyncMock

    from agent_trader.data.market import fetch_candles

    fake = [Candle(timestamp_ms=1000, open=1, high=2, low=0.5, close=1.5, volume=10)]

    with (
        patch("agent_trader.data.cache.load_cached_candles", return_value=None),
        patch("agent_trader.data.bybit.fetch_bybit_candles", new_callable=AsyncMock, return_value=[]),
        patch("agent_trader.data.market._fetch_hl_candles", new_callable=AsyncMock, return_value=[]),
        patch("agent_trader.data.s3_fills.fetch_s3_candles", new_callable=AsyncMock, return_value=fake),
        patch("agent_trader.data.cache.save_to_cache"),
    ):
        result = await fetch_candles("xyz:GOLD", 1000, 2000, "1m")
        assert len(result) == 1
        assert result[0].close == 1.5
