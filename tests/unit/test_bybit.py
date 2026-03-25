import pytest

from agent_trader.data.bybit import hl_to_bybit_symbol


@pytest.mark.parametrize("hl,expected", [
    ("BTC", "BTCUSDT"),
    ("ETH", "ETHUSDT"),
    ("SOL", "SOLUSDT"),
    ("kPEPE", "1000PEPEUSDT"),
    ("kBONK", "1000BONKUSDT"),
    ("kFLOKI", "1000FLOKIUSDT"),
    ("kSHIB", "SHIB1000USDT"),
    ("kLUNC", "1000LUNCUSDT"),
    ("kNEIRO", "1000NEIROCTOUSDT"),
    ("RNDR", "RENDERUSDT"),
    ("FTM", "SONICUSDT"),
    ("MATIC", "POLUSDT"),
    ("TURBO", "1000TURBOUSDT"),
    ("JELLY", "JELLYJELLYUSDT"),
    ("HPOS", "HPOS10IUSDT"),
    ("TST", "TSTBSCUSDT"),
    ("xyz:GOLD", None),
    ("cash:WTI", None),
    ("vntl:SPACEX", None),
    ("flx:OIL", None),
    ("km:NVDA", None),
])
def test_hl_to_bybit_symbol(hl, expected):
    assert hl_to_bybit_symbol(hl) == expected
