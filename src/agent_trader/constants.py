TIMEFRAME_MS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}

CHART_WINDOW = {
    "5m":  {"interval": "1m", "pad_ms": 10 * 60 * 1000},
    "15m": {"interval": "1m", "pad_ms": 30 * 60 * 1000},
    "30m": {"interval": "1m", "pad_ms": 60 * 60 * 1000},
    "1h":  {"interval": "1m", "pad_ms": 2 * 60 * 60 * 1000},
    "4h":  {"interval": "5m", "pad_ms": 8 * 60 * 60 * 1000},
}
