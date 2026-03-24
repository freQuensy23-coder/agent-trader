# MITM Proxy — Public Read API Coverage

Public read-only endpoints for market data. No account, orders, positions, or write operations.

All endpoint paths and parameters verified against official documentation (2026-03-24).

All 47 endpoints individually verified by dedicated agents (existence, proxy strategy validity, no bulk download required). Zero discrepancies found.

---

## Bybit v5 — Public Market Data (22 endpoints)

Official docs list exactly 22 endpoints under `/v5/market/`.

### Endpoints with historical data (can time-cap)

These endpoints accept time range parameters. Proxy strategy: **forward to real Bybit, inject time cap to prevent future data leakage**.

| Endpoint | Description | Time Params | Proxy Strategy |
|----------|------------|-------------|----------------|
| `GET /v5/market/kline` | OHLCV candles | `start`, `end` (ms) | **Forward + time cap** (`end=T`) |
| `GET /v5/market/mark-price-kline` | Mark price candles | `start`, `end` (ms) | **Forward + time cap** (`end=T`) |
| `GET /v5/market/index-price-kline` | Index price candles | `start`, `end` (ms) | **Forward + time cap** (`end=T`) |
| `GET /v5/market/premium-index-price-kline` | Premium index candles (basis for funding) | `start`, `end` (ms) | **Forward + time cap** (`end=T`) |
| `GET /v5/market/funding/history` | Funding rate history | `startTime`, `endTime` (ms) | **Forward + time cap** (`endTime=T`) |
| `GET /v5/market/open-interest` | Open interest history | `startTime`, `endTime` (ms) | **Forward + time cap** (`endTime=T`) |
| `GET /v5/market/account-ratio` | Long/short ratio | `startTime`, `endTime` (ms) | **Forward + time cap** (`endTime=T`) |
| `GET /v5/market/historical-volatility` | Historical volatility (options, hourly, up to 2 years) | `startTime`, `endTime` (ms) | **Forward + time cap** (`endTime=T`). Options only — block if agent trades perps only. Note: `startTime`/`endTime` must be passed as a pair, max 30-day range per query. |

### Current-state endpoints (no time params)

| Endpoint | Description | Proxy Strategy | Issues |
|----------|------------|----------------|--------|
| `GET /v5/market/tickers` | Price, 24h stats, funding rate, OI | **Intercept** — construct from kline + funding/history + open-interest at T | Funding rate and OI must come from real historical data, not hardcoded. Known inaccuracies: bid/ask spread is synthetic, markPrice and indexPrice are approximated as lastPrice. |
| `GET /v5/market/orderbook` | Order book snapshot | **Cannot proxy correctly.** No historical API. Free historical data only via bulk file download from Bybit portal (not API). | Block or synthetic |
| `GET /v5/market/recent-trade` | Recent public trades | **Cannot proxy.** Returns only current trades, no time filter. | Block |
| `GET /v5/market/time` | Server time (seconds + nanoseconds) | **Intercept** — return backtest time T | None |
| `GET /v5/market/instruments-info` | Instrument specs (tick size, lot size, leverage limits) | **Passthrough** | Specs may differ from historical values: new listings didn't exist at T, leverage limits may have changed, pre-market contracts have different phases. |
| `GET /v5/market/risk-limit` | Risk limit tiers per symbol | **Passthrough** | None |
| `GET /v5/market/insurance` | Insurance fund balance | **Passthrough** | None |
| `GET /v5/market/index-price-components` | Components of the index price | **Passthrough** | None |
| `GET /v5/market/price-limit` | Min/max order price limits | **Passthrough** | None |
| `GET /v5/market/fee-group-info` | Fee group structure | **Passthrough** | Requires `productType` param (must be "contract" for derivatives). |
| `GET /v5/market/delivery-price` | Delivery price records (expiring futures/options) | **Block** | Not needed for perpetuals |
| `GET /v5/market/new-delivery-price` | Delivery price (new format, options only) | **Block** | Not needed for perpetuals |
| `GET /v5/market/rpi_orderbook` | RPI orderbook (market maker infrastructure) | **Block** | Not needed |
| `GET /v5/market/adlAlert` | Auto-deleverage warnings (updated every 1 min) | **Block** | Current state only, no historical data |

### Bybit summary

| Category | Count | Endpoints |
|----------|-------|-----------|
| Forward + time cap | 8 | kline, mark-price-kline, index-price-kline, premium-index-price-kline, funding/history, open-interest, account-ratio, historical-volatility |
| Intercept (construct locally) | 2 | tickers, time |
| Passthrough (static/rare change) | 6 | instruments-info, risk-limit, insurance, index-price-components, price-limit, fee-group-info |
| Cannot proxy | 2 | orderbook, recent-trade |
| Block (not needed) | 4 | delivery-price, new-delivery-price, rpi_orderbook, adlAlert |

---

## HyperLiquid — Public Read API (`POST /info`, 25 types)

All requests: `POST https://api.hyperliquid.xyz/info` with `Content-Type: application/json`.

No authentication required for any of these types.

### Endpoints with historical data (can time-cap)

Only 2 public types return historical data with time parameters.

| `type` | Description | Params | Proxy Strategy |
|--------|------------|--------|----------------|
| `candleSnapshot` | Historical OHLCV candles | `req: {coin, interval, startTime, endTime}` | **Forward + time cap** (`endTime=T`) |
| `fundingHistory` | Historical funding rates per coin | `coin`, `startTime`, `endTime` (opt) | **Forward + time cap** (`endTime=T`) |

### Market data (current state)

| `type` | Description | Params | Proxy Strategy | Issues |
|--------|------------|--------|----------------|--------|
| `meta` | Perp metadata: asset list, decimals, tick sizes | `dex` (opt) | **From local file** | None |
| `metaAndAssetCtxs` | Metadata + live context (funding, OI, mark price) | `dex` (opt) | **From local file filtered by T** | Snapshots must be pre-collected |
| `allMids` | Current mid-prices for all assets | `dex` (opt) | **Approximate** — use close from candleSnapshot at T | Not exact. Low-volatility: negligible difference. High-volatility: 0.1-1%+ gap between candle close and actual mid-price. |
| `l2Book` | L2 order book | `coin`, `nSigFigs` (opt), `mantissa` (opt) | **Cannot proxy.** Current state only, no historical API. | Block or synthetic |
| `predictedFundings` | Predicted funding rates for next period | none | **Cannot proxy.** Forward-looking only. | Block |
| `perpsAtOpenInterestCap` | Assets currently at OI cap | `dex` (opt) | **Cannot proxy.** Current state only. | Block |
| `spotMeta` | Spot token metadata | none | **From local file** | None |
| `spotMetaAndAssetCtxs` | Spot metadata + live context | none | **From local file filtered by T** | Snapshots must be pre-collected |

### Perp reference data (static)

| `type` | Description | Params | Proxy Strategy |
|--------|------------|--------|----------------|
| `allPerpMetas` | Metadata for all perps across all DEXs | none | **From local file** |
| `perpCategories` | Array of [coin, category] pairs | none | **Passthrough** |
| `perpAnnotation` | Category and description for a specific perp | `coin` | **Passthrough** |
| `perpConciseAnnotations` | Array of [coin, {category, keywords}] | none | **Passthrough** |

### DEX infrastructure (not needed)

| `type` | Description | Params | Proxy Strategy |
|--------|------------|--------|----------------|
| `perpDexs` | List of all perp DEXs | none | **Block** |
| `perpDeployAuctionStatus` | Perp deploy auction info | none | **Block** |
| `perpDexLimits` | OI caps for builder-deployed DEX | `dex` | **Block** |
| `perpDexStatus` | Total net deposit for a DEX | `dex` | **Block** |
| `spotPairDeployAuctionStatus` | Spot pair deploy auction info | none | **Block** |

### Lending / vaults / tokens (not needed)

| `type` | Description | Params | Proxy Strategy |
|--------|------------|--------|----------------|
| `vaultDetails` | Vault info (AUM, PnL, positions) | `vaultAddress` | **Block** |
| `tokenDetails` | HIP token info (supply, pricing) | `tokenId` | **Block** |
| `borrowLendReserveState` | Borrow/supply rates for a token | `token` (index) | **Block** |
| `allBorrowLendReserveStates` | Reserve state for all tokens | none | **Block** |
| `alignedQuoteTokenInfo` | Alignment info and predicted rate | `token` (index) | **Block** |
| `outcomeMeta` | Prediction market metadata (testnet only) | none | **Block** |

### HyperLiquid summary

| Category | Count | Types |
|----------|-------|-------|
| Forward + time cap | 2 | candleSnapshot, fundingHistory |
| From local file | 5 | meta, metaAndAssetCtxs, allPerpMetas, spotMeta, spotMetaAndAssetCtxs |
| Approximate | 1 | allMids |
| Passthrough (static) | 3 | perpCategories, perpAnnotation, perpConciseAnnotations |
| Cannot proxy | 3 | l2Book, predictedFundings, perpsAtOpenInterestCap |
| Block (not needed) | 11 | perpDexs, perpDeployAuctionStatus, perpDexLimits, perpDexStatus, spotPairDeployAuctionStatus, vaultDetails, tokenDetails, borrowLendReserveState, allBorrowLendReserveStates, alignedQuoteTokenInfo, outcomeMeta |

---

## Combined Summary

### What can be proxied

| Strategy | Bybit | HyperLiquid | Total |
|----------|-------|-------------|-------|
| Forward + time cap | 8 | 2 | 10 |
| Intercept (construct locally) | 2 | 0 | 2 |
| From local file | 0 | 5 | 5 |
| Approximate | 0 | 1 | 1 |
| Passthrough | 6 | 3 | 9 |
| **Subtotal: can proxy** | **16** | **11** | **27** |

### What cannot be proxied (no free historical API)

| Endpoint | Exchange | Reason |
|----------|----------|--------|
| `orderbook` | Bybit | Current state only. No historical API. Free bulk download exists (not API). |
| `recent-trade` | Bybit | Current state only. No time filter parameter. |
| `l2Book` | HyperLiquid | Current state only. No historical API. |
| `predictedFundings` | HyperLiquid | Forward-looking only. Historical predictions not stored. |
| `perpsAtOpenInterestCap` | HyperLiquid | Current state only. No historical snapshots. |

### What is blocked (irrelevant for backtest)

| Exchange | Count | Categories |
|----------|-------|------------|
| Bybit | 4 | delivery-price, new-delivery-price, rpi_orderbook, adlAlert |
| HyperLiquid | 11 | DEX infrastructure, lending, vaults, tokens, testnet |

---

## Proxy Strategy Definitions

| Strategy | Description |
|----------|------------|
| **Forward + time cap** | Forward to real API, inject `end=T` or `endTime=T` to cap responses at backtest time. Return real response unchanged. |
| **Intercept** | Do not forward. Construct response locally from other API calls or tracker state. |
| **From local file** | Serve pre-collected data snapshots filtered by backtest time T. Data must be collected before backtest run. |
| **Approximate** | Construct best-effort response from available data. Not exact — document the approximation. |
| **Passthrough** | Forward as-is to real API. For static or rarely-changing data where time filtering is not needed. |
| **Block** | Return error response. Endpoint is either irrelevant or impossible to proxy correctly. |

---

## Path Gotchas (Bybit)

Several Bybit endpoint paths differ from what you might expect:

| Common mistake | Actual API path |
|----------------|----------------|
| `/v5/market/long-short-ratio` | `/v5/market/account-ratio` |
| `/v5/market/order-price-limit` | `/v5/market/price-limit` |
| `/v5/market/rpi-orderbook` | `/v5/market/rpi_orderbook` |
| `/v5/market/adl-alert` | `/v5/market/adlAlert` |
| `/v5/market/index-components` | `/v5/market/index-price-components` |
| `/v5/market/history-fund-rate` | `/v5/market/funding/history` |
| `/v5/market/iv` | `/v5/market/historical-volatility` |

---

## Testing Strategy

47 endpoints × 2 = 94 tests total. Each endpoint has both a unit test and an integration test.

### Unit tests (`tests/unit/test_proxy_unit.py`)

- Real mitmproxy on localhost
- Real SDK (pybit for Bybit, httpx for HyperLiquid) sending requests through the proxy
- **Mocked** external APIs — the httpx client inside MITMProxyAddon is replaced with prepared JSON responses
- Fast, offline, deterministic

### Integration tests (`tests/integration/test_proxy_integration.py`)

- Real mitmproxy on localhost
- Real SDK (pybit for Bybit, httpx for HyperLiquid) sending requests through the proxy
- **Real** Bybit and HyperLiquid APIs — requests go to actual servers
- Slow, requires network, validates that real APIs return expected format

### Architecture

```
Unit:                               Integration:

Real SDK                            Real SDK
  ↓ real HTTP                         ↓ real HTTP
Real mitmproxy                      Real mitmproxy
  ↓ intercept                         ↓ intercept
Real MITMProxyAddon                 Real MITMProxyAddon
  ↓                                   ↓
Mocked API responses                Real Bybit / HyperLiquid API
```

### Directory structure

```
tests/
  unit/
    test_proxy_unit.py          # 47 tests, mocked APIs, fast
    fixtures/                   # prepared JSON responses
  integration/
    test_proxy_integration.py   # 47 tests, real APIs, slow
```

### Running

```bash
pytest tests/unit/                    # fast, offline
pytest tests/integration/             # slow, needs network
pytest                                # all 94 tests
```

### What each test asserts per category

| Category | Count | Unit test asserts | Integration test asserts |
|----------|-------|-------------------|------------------------|
| Forward + time cap | 10 | All timestamps <= T (from mock data) | All timestamps <= T (from real API) |
| Intercept (construct) | 2 | Valid structure, correct values | Valid structure, correct values |
| From local file | 5 | Valid HL response format | Valid HL response format |
| Approximate | 1 | Returns prices > 0 | Returns prices > 0, close to real mid |
| Passthrough | 9 | Valid response structure | Valid response structure from real API |
| Cannot proxy | 5 | Returns 403, not forwarded | Returns 403, not forwarded |
| Block (not needed) | 15 | Returns 403 | Returns 403 |
