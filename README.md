# agent-trader

Backtest harness for a Claude agent that reads Trump Truth Social posts, decides whether they'll move a market, and writes trade predictions. Predictions are scored against historical HyperLiquid / Bybit candles.

The agent runs behind a mitmproxy that caps every HTTP response to the post timestamp, so it can call live-looking market APIs without seeing data from after the post was published.

## Requirements

- Python 3.12+
- `uv` (project uses `uv.lock`)
- mitmproxy CA cert installed at `~/.mitmproxy/mitmproxy-ca-cert.pem` (run `mitmproxy` once to generate it)
- `.env` with `ANTHROPIC_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (the S3 fills fallback uses requester-pays on `hl-mainnet-node-data`)

## First-time setup

```bash
uv sync
python scripts/collect_snapshots.py       # writes data/proxy_snapshots/{meta,allPerpMetas,spotMeta}.json
python scripts/fetch_gdelt_news.py        # data/news/headlines.json (optional, used as prompt context)
```

Missing snapshots = proxy returns HTTP 403. There is no silent fallback to the live HL API — this is deliberate, so a stale snapshot can't leak through.

## Run a backtest

```bash
# full run against data/posts/trump_posts.parquet
uv run python -m agent_trader.main run

# constrained by time
uv run python -m agent_trader.main run --start=2025-01-01 --end=2025-02-01 --concurrency=3

# single-post smoke test
uv run python -m agent_trader.main run --posts_path=data/posts/test_crypto.parquet --concurrency=1
```

Reports land in `data/results/{run_id}.html`. Traces go to W&B Weave under project `agent-trader` (set `wandb_project=...` to override).

CLI args beyond `--start` / `--end` are passed straight into `BacktestConfig` (`src/agent_trader/config.py`), so anything defined there is overridable — `--model`, `--max_budget_per_post_usd`, `--proxy_base_port`, etc.

## How a single post is processed

1. `BacktestEngine._process_post` (`src/agent_trader/backtest/engine.py:193`) picks a worker slot (one mitmproxy + one MCP server per worker) and POSTs `/__control/set_time` with the post's timestamp.
2. Claude gets `build_system_prompt()` + `build_user_prompt()` (`src/agent_trader/agent/prompts.py`). The user prompt includes the last 5 posts and the past 7 days of GDELT headlines.
3. The agent is allowed `Bash` and the MCP tool `submit_recommendation` (`src/agent_trader/agent/tool.py`). It calls HL / Bybit via its sandbox proxy; every response is capped to the current `T`.
4. The recommendation is a `Recommendation` model (`src/agent_trader/models/recommendation.py`): `action` ∈ {signal, skip}, up to 3 `AssetPrediction`s, `importance_score` 1–10. Validators enforce: `signal` ⇒ ≥1 prediction; `importance_score < 7` ⇒ must skip; asset must exist in `allPerpMetas.json`.
5. `Evaluator.evaluate` (`src/agent_trader/backtest/evaluator.py:13`) fetches the 1m candle at `T` and at `T + timeframe`, computes % change, marks direction correct/wrong. If either candle is missing it sets `skipped=True` with a reason — that's the signal you need to chase in the logs.

## Candle fetch fallback

`data.market.fetch_candles` (`src/agent_trader/data/market.py:76`) walks these in order and caches every success:

1. Parquet cache at `data/cache/candles/{asset}/{interval}.parquet`
2. Aggregate cached 1m candles into the requested interval
3. Bybit kline API (through the proxy, with HL→Bybit symbol mapping in `data/bybit.py:22` — `kPEPE→1000PEPEUSDT`, `RNDR→RENDERUSDT`, etc. Builder-DEX assets like `xyz:GOLD` skip Bybit.)
4. HL `candleSnapshot` (paginated 500/req)
5. HL S3 fills, aggregated into OHLCV. Three on-disk formats by date (`data/s3_fills.py`): `node_trades` before 2025-05-25, `node_fills` through 2025-07-27, `node_fills_by_block` after.

Do **not** `rm -rf data/cache/candles` — rebuilding the S3 branch is slow and hits AWS egress.

## Proxy behaviour

`src/agent_trader/proxy/addon.py` runs as a mitmdump addon.

- HL `candleSnapshot`, `allMids`, `metaAndAssetCtxs`, `fundingHistory` → served from cache capped to `T`.
- HL `meta` / `allPerpMetas` / `spotMeta` → served from `data/proxy_snapshots/`.
- Bybit `kline`, `mark-price-kline`, `open-interest`, `account-ratio` → live call with `end` / `endTime` rewritten to `min(T, original)`.
- Bybit `tickers` → synthesized from the cached candle at `T`.
- `l2Book`, `orderbook`, recent trades → blocked (non-deterministic, can't reproduce historically).

Control endpoints: `POST /__control/set_time` (body: `{"time_ms": ...}`) and `GET /__control/get_time` (used by `engine.py` as a readiness probe).

## Tests

```bash
uv run python -m pytest tests/unit/ -v
```

Unit tests cover the cache, Bybit client (respx), post parsing, the 3 S3 fill formats, and the fallback chain. They do not cover the agent or the proxy — for those, run the smoke test:

```bash
uv run python -m agent_trader.main run --posts_path=data/posts/test_crypto.parquet --concurrency=1
```

and grep the trace for:

- `Bybit: {asset}/1m → N candles` on every prediction (success)
- `No candle data: ...` (one of the fallbacks exhausted itself)
- `skipped=True` in outcomes (evaluator couldn't price a prediction — the prediction doesn't count)
- 240 candles on 1h chart panels in the HTML report (confirms pagination)

## Layout

```
src/agent_trader/
  backtest/engine.py       BacktestEngine: proxy pool, worker loop, report
  backtest/evaluator.py    direction-correctness scoring
  agent/prompts.py         system + user prompts
  agent/tool.py            submit_recommendation MCP tool
  data/market.py           fetch_candles (4-level fallback)
  data/bybit.py            kline client + HL→Bybit symbol map
  data/cache.py            parquet cache: candles, funding, L/S ratio
  data/s3_fills.py         HL S3 fills → OHLCV
  data/posts.py            parquet/JSON post loader
  data/news.py             GDELT headlines context
  proxy/addon.py           mitmproxy addon, time-capped
  models/                  Pydantic schemas (Recommendation, PostResult, TruthPost)
  reporting/html_report.py Jinja2 HTML report
  config.py                BacktestConfig (pydantic-settings)
  main.py                  fire entrypoint
scripts/
  collect_snapshots.py     one-time: fetch HL meta snapshots
  prefetch_candles.py      warm the candle cache before a run
  fetch_gdelt_news.py      build data/news/headlines.json
  fetch_weave_trace.py     debug: pull a Weave trace locally
data/
  posts/                   trump_posts.parquet + test_*.parquet fixtures
  cache/                   parquet caches — keep this
  proxy_snapshots/         HL metadata (required)
  results/                 generated HTML reports
```

## Gotchas

- Concurrency ≠ free: each worker spawns its own mitmproxy on `proxy_base_port + i`, so `concurrency=5` claims ports 8080–8084.
- The agent prompt (`agent/prompts.py`) hard-codes the allowed timeframes (`5m/15m/30m/1h/4h`) and confidence levels (`high/very_high`). The Pydantic model enforces the same set; changing one without the other will silently fail validation.
- Asset names are validated against `allPerpMetas.json` at runtime. If you regenerate snapshots and an asset is missing, existing recommendations referencing it will fail to load.
- `max_budget_per_post_usd=None` is unlimited. Set it before running `trump_posts.parquet` against Opus.
