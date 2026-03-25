# Agent Trader

Trump post trading agent backtest system. Analyzes Trump's social media posts, predicts market impact, evaluates against real price data.

## Architecture

```
Post → Agent (Claude via proxy) → submit_recommendation → Evaluator → Report
         ↕                                                    ↕
    mitmproxy (time-capped)                          fetch_candles()
    ├─ Bybit API (passthrough, end capped)           ├─ 1. Cache (parquet)
    └─ HL API (intercepted via fetch_candles)         ├─ 2. Bybit API
                                                      ├─ 3. HL API
                                                      └─ 4. S3 fills
```

## Key Files

| File | Purpose |
|------|---------|
| `backtest/engine.py` | Orchestration: proxy, agent, evaluator, report |
| `backtest/evaluator.py` | Checks prediction accuracy via fetch_price_change |
| `proxy/addon.py` | mitmproxy addon, enforces time isolation |
| `data/market.py` | fetch_candles: 4-level fallback chain |
| `data/bybit.py` | Bybit kline API, symbol mapping |
| `data/cache.py` | Parquet candle cache |
| `data/s3_fills.py` | S3 fills download, OHLCV aggregation |
| `agent/prompts.py` | System/user prompts |
| `agent/tool.py` | MCP tool: submit_recommendation |
| `models/recommendation.py` | Pydantic schema, asset validation |

## Running

```bash
# Collect snapshots first (one-time)
python scripts/collect_snapshots.py

# Full backtest
uv run python -c "
import asyncio
from agent_trader.backtest.engine import BacktestEngine
from agent_trader.config import BacktestConfig
config = BacktestConfig(posts_path='data/posts/trump_posts.parquet', concurrency=3)
asyncio.run(BacktestEngine(config).run())
"

# Single post test
uv run python -c "
import asyncio
from agent_trader.backtest.engine import BacktestEngine
from agent_trader.config import BacktestConfig
config = BacktestConfig(posts_path='data/posts/test_crypto.parquet', concurrency=1)
asyncio.run(BacktestEngine(config).run())
"
```

## Testing

### Unit tests
```bash
uv run python -m pytest tests/unit/ -v
```

### Integration testing (the real test)

Unit tests are not enough. Always run the full backtest and verify:

1. **Agent makes signals** — on obvious market-moving posts, action=signal
2. **API requests succeed** — check logs for `Bybit: X/1m → N candles`, no empty responses
3. **Evaluator produces outcomes** — no skipped predictions (check `skipped=True` in outcomes)
4. **Charts have data** — 240 candles for 1h timeframe (pagination works)

#### Test datasets

| File | Posts | Purpose |
|------|-------|---------|
| `test_crypto.parquet` | 1 | Quick smoke test (crypto post, should signal) |
| `test_signals.parquet` | 8 | Obvious market-moving posts across categories |
| `test_one.parquet` | 1 | Tariff post |

#### What to check in traces

```
# Good: candles fetched successfully
Bybit: BTC/1m → 1 candles
Bybit: BTC/1m → 240 candles   # chart candles, pagination worked

# Bad: empty response
No candle data: xyz:GOLD/1m [...]   # fallback chain exhausted

# Bad: skipped prediction
No price data for ASSET @ timestamp: ...   # evaluator couldn't evaluate
```

#### Smoke test checklist
1. Clear cache: `rm -rf data/cache/candles/`
2. Run: `posts_path='data/posts/test_crypto.parquet' concurrency=1`
3. Verify: agent signals, all candle fetches succeed, direction accuracy reported
4. Check report: `data/results/*.html`

## Proxy

Snapshots required in `data/proxy_snapshots/`:
- `meta.json`, `allPerpMetas.json`, `spotMeta.json`
- Generate: `python scripts/collect_snapshots.py`

Missing snapshots = hard 403 error (no silent fallback to live API).

## Bybit Symbol Mapping

HL assets map to Bybit via `hl_to_bybit_symbol()`:
- `kPEPE` → `1000PEPEUSDT`, `RNDR` → `RENDERUSDT`, etc.
- Builder DEX assets (`xyz:GOLD`, `cash:WTI`) → `None` (Bybit skipped)

## Dependencies

boto3 + lz4 required for S3 fills. AWS credentials needed in `.env`:
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```
