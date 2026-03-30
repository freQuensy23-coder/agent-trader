"""
mitmproxy addon for backtesting. Intercepts HTTP traffic from the agent sandbox
and enforces time isolation: the agent cannot see data after backtest time T.

All intercepted endpoints serve data from local cache — zero outgoing network calls.
Bybit passthrough endpoints (kline, OI, etc.) are time-capped and forwarded.

Usage:
    mitmdump --listen-port 8080 -s src/agent_trader/proxy/addon.py --set data_dir=data/proxy_snapshots
"""

import json
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from mitmproxy import ctx, http

from agent_trader.data.bybit import _RENAMES
from agent_trader.data.cache import (
    load_cached_candles,
    load_cached_funding,
    load_cached_ls_ratio,
    load_latest_funding,
    load_latest_ls_ratio,
    load_nearest_cached_candle,
)

BYBIT_TIME_CAP_END = {
    "/v5/market/kline",
    "/v5/market/mark-price-kline",
    "/v5/market/index-price-kline",
    "/v5/market/premium-index-price-kline",
}

BYBIT_TIME_CAP_ENDTIME = {
    "/v5/market/funding/history",
    "/v5/market/open-interest",
    "/v5/market/historical-volatility",
}

BYBIT_INTERCEPT = {
    "/v5/market/tickers",
    "/v5/market/account-ratio",
}

BYBIT_PASSTHROUGH = {
    "/v5/market/instruments-info",
    "/v5/market/risk-limit",
    "/v5/market/insurance",
    "/v5/market/index-price-components",
    "/v5/market/price-limit",
    "/v5/market/fee-group-info",
}

BYBIT_BLOCK = {
    "/v5/market/orderbook",
    "/v5/market/recent-trade",
    "/v5/market/delivery-price",
    "/v5/market/new-delivery-price",
    "/v5/market/rpi_orderbook",
    "/v5/market/adlAlert",
}

HL_INTERCEPT = {
    "candleSnapshot", "allMids", "metaAndAssetCtxs", "spotMetaAndAssetCtxs",
    "fundingHistory",
}

HL_FROM_LOCAL_FILE = {"meta", "allPerpMetas", "spotMeta"}

HL_PASSTHROUGH = {"perpCategories", "perpAnnotation", "perpConciseAnnotations"}

HL_BLOCK = {
    "l2Book", "predictedFundings", "perpsAtOpenInterestCap",
    "perpDexs", "perpDeployAuctionStatus", "perpDexLimits", "perpDexStatus",
    "spotPairDeployAuctionStatus",
    "vaultDetails", "tokenDetails", "borrowLendReserveState",
    "allBorrowLendReserveStates", "alignedQuoteTokenInfo", "outcomeMeta",
}

# Reverse symbol map: Bybit base -> HL asset name
_RENAMES_REVERSE = {v: k for k, v in _RENAMES.items()}

MS_24H = 24 * 60 * 60_000


def _fmt(v: float) -> str:
    return f"{v:.10f}".rstrip("0").rstrip(".")


def _bybit_symbol_to_hl(symbol: str) -> str | None:
    if not symbol.endswith("USDT"):
        return None
    base = symbol[:-4]
    return _RENAMES_REVERSE.get(base, base)


def _json_response(flow: http.HTTPFlow, data, status: int = 200):
    body = json.dumps(data).encode()
    flow.response = http.Response.make(status, body, {"Content-Type": "application/json"})


def _block(flow: http.HTTPFlow, reason: str):
    flow.response = http.Response.make(
        403, json.dumps({"error": reason}).encode(), {"Content-Type": "application/json"}
    )


class BacktestProxy:
    def __init__(self):
        self.T: int | None = None
        self.data_dir: Path | None = None

    def load(self, loader):
        loader.add_option("data_dir", str, "", "Path to pre-collected proxy snapshots")

    def configure(self, updates):
        if "data_dir" in updates:
            d = ctx.options.data_dir
            if d:
                self.data_dir = Path(d)

    async def request(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        path = flow.request.path.split("?")[0]

        if "/__control/" in flow.request.path:
            self._handle_control(flow)
            return

        if "api.bybit.com" in host:
            await self._handle_bybit(flow, path)
            return

        if "api.hyperliquid.xyz" in host:
            await self._handle_hyperliquid(flow)
            return

        _block(flow, f"Domain not allowed: {host}")

    def _handle_control(self, flow: http.HTTPFlow):
        path = flow.request.path
        if path.endswith("/set_time"):
            try:
                body = json.loads(flow.request.content)
                self.T = int(body["timestamp_ms"])
                ctx.log.info(f"Backtest time set to {self.T}")
                _json_response(flow, {"ok": True, "T": self.T})
            except Exception as e:
                _json_response(flow, {"error": str(e)}, 400)
        elif path.endswith("/get_time"):
            _json_response(flow, {"T": self.T})
        else:
            _json_response(flow, {"error": "unknown control endpoint"}, 404)

    # --- Bybit ---

    async def _handle_bybit(self, flow: http.HTTPFlow, path: str):
        if path in BYBIT_TIME_CAP_END:
            self._bybit_time_cap(flow, "end")
        elif path in BYBIT_TIME_CAP_ENDTIME:
            self._bybit_time_cap(flow, "endTime")
        elif path in BYBIT_INTERCEPT:
            self._bybit_intercept(flow, path)
        elif path == "/v5/market/time":
            self._bybit_intercept_time(flow)
        elif path in BYBIT_PASSTHROUGH:
            pass
        elif path in BYBIT_BLOCK:
            _block(flow, f"Bybit endpoint blocked: {path}")
        else:
            _block(flow, f"Unknown Bybit endpoint: {path}")

    def _bybit_time_cap(self, flow: http.HTTPFlow, param_name: str):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        parsed = urlparse(flow.request.url)
        params = parse_qs(parsed.query, keep_blank_values=True)

        current_end = params.get(param_name, [None])[0]
        if current_end is None or int(current_end) > self.T:
            params[param_name] = [str(self.T)]

        new_query = urlencode(params, doseq=True)
        flow.request.url = urlunparse(parsed._replace(query=new_query))

    def _bybit_intercept_time(self, flow: http.HTTPFlow):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return
        t_sec = self.T // 1000
        t_nano = str(self.T * 1_000_000)
        _json_response(flow, {
            "retCode": 0, "retMsg": "OK",
            "result": {"timeSecond": str(t_sec), "timeNano": t_nano},
            "retExtInfo": {}, "time": self.T,
        })

    def _bybit_intercept(self, flow: http.HTTPFlow, path: str):
        if path == "/v5/market/tickers":
            self._bybit_intercept_tickers(flow)
        elif path == "/v5/market/account-ratio":
            self._bybit_intercept_account_ratio(flow)

    def _bybit_intercept_tickers(self, flow: http.HTTPFlow):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        parsed = urlparse(flow.request.url)
        params = parse_qs(parsed.query)
        category = params.get("category", ["linear"])[0]
        symbol = params.get("symbol", [None])[0]

        if not symbol:
            _block(flow, "tickers requires symbol param")
            return

        hl_asset = _bybit_symbol_to_hl(symbol)

        last_price = "0"
        prev_price_24h = "0"
        high_24h = "0"
        low_24h = "0"
        volume_24h = "0"
        funding_rate = "0"

        if hl_asset:
            candles = load_cached_candles(hl_asset, "1m", self.T - 60_000, self.T)
            if candles:
                last_price = str(candles[-1].close)
            elif (nearest := load_nearest_cached_candle(hl_asset, self.T)):
                last_price = str(nearest.close)

            candles_24h = load_cached_candles(hl_asset, "1m", self.T - MS_24H, self.T)
            if candles_24h:
                prev_price_24h = str(candles_24h[0].open)
                high_24h = str(max(c.high for c in candles_24h))
                low_24h = str(min(c.low for c in candles_24h))
                volume_24h = str(sum(c.volume for c in candles_24h))

            fr = load_latest_funding(hl_asset, self.T)
            if fr is not None:
                funding_rate = _fmt(fr)

        price_pct = "0"
        try:
            lp = float(last_price)
            pp = float(prev_price_24h)
            if pp > 0:
                price_pct = _fmt((lp - pp) / pp)
        except (ValueError, ZeroDivisionError):
            pass

        ticker = {
            "symbol": symbol,
            "lastPrice": last_price,
            "markPrice": last_price,
            "indexPrice": last_price,
            "prevPrice24h": prev_price_24h,
            "price24hPcnt": price_pct,
            "highPrice24h": high_24h,
            "lowPrice24h": low_24h,
            "turnover24h": "0",
            "volume24h": volume_24h,
            "fundingRate": funding_rate,
            "nextFundingTime": "0",
            "openInterest": "0",
            "openInterestValue": "0",
            "bid1Price": last_price,
            "bid1Size": "1",
            "ask1Price": last_price,
            "ask1Size": "1",
        }
        _json_response(flow, {
            "retCode": 0, "retMsg": "OK",
            "result": {"category": category, "list": [ticker]},
            "retExtInfo": {}, "time": self.T,
        })

    def _bybit_intercept_account_ratio(self, flow: http.HTTPFlow):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        parsed = urlparse(flow.request.url)
        params = parse_qs(parsed.query)
        symbol = params.get("symbol", [None])[0]
        start_time = int(params.get("startTime", ["0"])[0])
        end_time = min(int(params.get("endTime", [str(self.T)])[0]), self.T)

        if not symbol:
            _block(flow, "account-ratio requires symbol param")
            return

        hl_asset = _bybit_symbol_to_hl(symbol)
        result_list = []

        if hl_asset:
            entries = load_cached_ls_ratio(hl_asset, start_time, end_time)
            if entries:
                for ts, buy, sell in entries:
                    result_list.append({
                        "symbol": symbol,
                        "buyRatio": str(buy),
                        "sellRatio": str(sell),
                        "timestamp": str(ts),
                    })

        _json_response(flow, {
            "retCode": 0, "retMsg": "OK",
            "result": {"list": result_list},
            "retExtInfo": {}, "time": self.T,
        })

    # --- HyperLiquid ---

    async def _handle_hyperliquid(self, flow: http.HTTPFlow):
        if flow.request.method != "POST":
            _block(flow, "HyperLiquid only accepts POST")
            return

        try:
            body = json.loads(flow.request.content)
        except Exception:
            _block(flow, "Invalid JSON body")
            return

        req_type = body.get("type", "")

        if req_type in HL_INTERCEPT:
            await self._hl_intercept(flow, body, req_type)
        elif req_type in HL_FROM_LOCAL_FILE:
            self._hl_from_local_file(flow, body, req_type)
        elif req_type in HL_PASSTHROUGH:
            pass
        elif req_type in HL_BLOCK:
            _block(flow, f"HyperLiquid type blocked: {req_type}")
        else:
            _block(flow, f"Unknown HyperLiquid type: {req_type}")

    async def _hl_intercept(self, flow: http.HTTPFlow, body: dict, req_type: str):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        if req_type == "candleSnapshot":
            self._hl_intercept_candles(flow, body)
        elif req_type == "allMids":
            self._hl_intercept_all_mids(flow)
        elif req_type in ("metaAndAssetCtxs", "spotMetaAndAssetCtxs"):
            self._hl_intercept_meta_and_ctxs(flow, body, req_type)
        elif req_type == "fundingHistory":
            self._hl_intercept_funding_history(flow, body)

    def _hl_intercept_candles(self, flow: http.HTTPFlow, body: dict):
        req = body.get("req", {})
        coin = req.get("coin", "")
        interval = req.get("interval", "1m")
        start_time = int(req.get("startTime", 0))
        end_time = min(int(req.get("endTime", self.T)), self.T)

        from agent_trader.data.cache import load_cached_candles_via_aggregation

        candles = load_cached_candles(coin, interval, start_time, end_time)
        if not candles and interval != "1m":
            candles = load_cached_candles_via_aggregation(coin, interval, start_time, end_time)
        if not candles:
            candles = []

        hl_format = [
            {"t": c.timestamp_ms, "o": _fmt(c.open), "h": _fmt(c.high),
             "l": _fmt(c.low), "c": _fmt(c.close), "v": _fmt(c.volume)}
            for c in candles
        ]
        _json_response(flow, hl_format)

    def _hl_intercept_all_mids(self, flow: http.HTTPFlow):
        if not self.data_dir or not (self.data_dir / "allPerpMetas.json").exists():
            _block(flow, "allPerpMetas.json not found. Run: python scripts/collect_snapshots.py")
            return

        all_metas = json.loads((self.data_dir / "allPerpMetas.json").read_text())
        assets = []
        for group in all_metas:
            for u in group.get("universe", []):
                assets.append(u["name"])

        mids = {}
        for asset in assets:
            candles = load_cached_candles(asset, "1m", self.T - 60_000, self.T)
            if candles:
                mids[asset] = _fmt(candles[-1].close)
            elif (nearest := load_nearest_cached_candle(asset, self.T)):
                mids[asset] = _fmt(nearest.close)

        _json_response(flow, mids)

    def _hl_intercept_meta_and_ctxs(self, flow: http.HTTPFlow, body: dict, req_type: str):
        if req_type == "spotMetaAndAssetCtxs":
            if self.data_dir and (self.data_dir / "spotMeta.json").exists():
                data = json.loads((self.data_dir / "spotMeta.json").read_text())
                _json_response(flow, [data, []])
            else:
                _block(flow, "spotMeta.json not found")
            return

        if not self.data_dir or not (self.data_dir / "meta.json").exists():
            _block(flow, "meta.json not found. Run: python scripts/collect_snapshots.py")
            return

        meta = json.loads((self.data_dir / "meta.json").read_text())
        assets = [u["name"] for u in meta.get("universe", [])]

        ctxs = []
        for asset in assets:
            mark_price = "0"
            funding = "0"

            candles = load_cached_candles(asset, "1m", self.T - 60_000, self.T)
            if candles:
                mark_price = _fmt(candles[-1].close)
            elif (nearest := load_nearest_cached_candle(asset, self.T)):
                mark_price = _fmt(nearest.close)

            fr = load_latest_funding(asset, self.T)
            if fr is not None:
                funding = _fmt(fr)

            ctxs.append({
                "funding": funding,
                "openInterest": "0",
                "prevDayPx": mark_price,
                "dayNtlVlm": "0",
                "premium": "0",
                "oraclePx": mark_price,
                "markPx": mark_price,
                "midPx": mark_price,
                "impactPxs": [mark_price, mark_price],
            })

        _json_response(flow, [meta, ctxs])

    def _hl_intercept_funding_history(self, flow: http.HTTPFlow, body: dict):
        coin = body.get("coin", "")
        start_time = int(body.get("startTime", 0))
        end_time = min(int(body.get("endTime", self.T)), self.T)

        entries = load_cached_funding(coin, start_time, end_time)

        if entries is None:
            _json_response(flow, [])
            return

        result = [
            {"coin": coin, "fundingRate": _fmt(rate), "premium": "0", "time": ts}
            for ts, rate in entries
        ]
        _json_response(flow, result)

    def _hl_from_local_file(self, flow: http.HTTPFlow, body: dict, req_type: str):
        if self.data_dir is None:
            _block(flow, f"No data_dir set, cannot serve {req_type}")
            return

        file_path = self.data_dir / f"{req_type}.json"
        if not file_path.exists():
            _block(flow, f"Snapshot not found: {file_path}. Run: python scripts/collect_snapshots.py")
            return

        data = json.loads(file_path.read_text())
        _json_response(flow, data)


addons = [BacktestProxy()]
