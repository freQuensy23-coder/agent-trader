"""
mitmproxy addon for backtesting. Intercepts HTTP traffic from the agent sandbox
and enforces time isolation: the agent cannot see data after backtest time T.

47 endpoints: Bybit (22) + HyperLiquid (25). See docs/mitm.md.

Usage:
    mitmdump --listen-port 8080 -s src/agent_trader/proxy/addon.py --set data_dir=data/proxy_snapshots
"""

import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from mitmproxy import ctx, http

# ---------------------------------------------------------------------------
# Bybit endpoint classifications
# ---------------------------------------------------------------------------

BYBIT_TIME_CAP_END = {
    "/v5/market/kline",
    "/v5/market/mark-price-kline",
    "/v5/market/index-price-kline",
    "/v5/market/premium-index-price-kline",
}

BYBIT_TIME_CAP_ENDTIME = {
    "/v5/market/funding/history",
    "/v5/market/open-interest",
    "/v5/market/account-ratio",
    "/v5/market/historical-volatility",
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

# ---------------------------------------------------------------------------
# HyperLiquid type classifications
# ---------------------------------------------------------------------------

HL_TIME_CAP = {"candleSnapshot", "fundingHistory"}

HL_FROM_LOCAL_FILE = {
    "meta", "metaAndAssetCtxs", "allPerpMetas", "spotMeta", "spotMetaAndAssetCtxs",
}

HL_PASSTHROUGH = {"perpCategories", "perpAnnotation", "perpConciseAnnotations"}

HL_BLOCK = {
    "l2Book", "predictedFundings", "perpsAtOpenInterestCap",
    "perpDexs", "perpDeployAuctionStatus", "perpDexLimits", "perpDexStatus",
    "spotPairDeployAuctionStatus",
    "vaultDetails", "tokenDetails", "borrowLendReserveState",
    "allBorrowLendReserveStates", "alignedQuoteTokenInfo", "outcomeMeta",
}


def _json_response(flow: http.HTTPFlow, data, status: int = 200):
    body = json.dumps(data).encode()
    flow.response = http.Response.make(status, body, {"Content-Type": "application/json"})


def _block(flow: http.HTTPFlow, reason: str):
    flow.response = http.Response.make(
        403, json.dumps({"error": reason}).encode(), {"Content-Type": "application/json"}
    )


class BacktestProxy:
    def __init__(self):
        self.T: int | None = None  # backtest time in ms
        self.data_dir: Path | None = None
        self._client = httpx.Client(timeout=15)

    def load(self, loader):
        loader.add_option("data_dir", str, "", "Path to pre-collected proxy snapshots")

    def configure(self, updates):
        if "data_dir" in updates:
            d = ctx.options.data_dir
            if d:
                self.data_dir = Path(d)

    def request(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        path = flow.request.path.split("?")[0]

        # --- Control endpoint ---
        if "/__control/" in flow.request.path:
            self._handle_control(flow)
            return

        # --- Bybit ---
        if "api.bybit.com" in host:
            self._handle_bybit(flow, path)
            return

        # --- HyperLiquid ---
        if "api.hyperliquid.xyz" in host:
            self._handle_hyperliquid(flow)
            return

        # --- Everything else: block ---
        _block(flow, f"Domain not allowed: {host}")

    # -----------------------------------------------------------------------
    # Control
    # -----------------------------------------------------------------------

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

    # -----------------------------------------------------------------------
    # Bybit
    # -----------------------------------------------------------------------

    def _handle_bybit(self, flow: http.HTTPFlow, path: str):
        if path in BYBIT_TIME_CAP_END:
            self._bybit_time_cap(flow, "end")
        elif path in BYBIT_TIME_CAP_ENDTIME:
            self._bybit_time_cap(flow, "endTime")
        elif path == "/v5/market/tickers":
            self._bybit_intercept_tickers(flow)
        elif path == "/v5/market/time":
            self._bybit_intercept_time(flow)
        elif path in BYBIT_PASSTHROUGH:
            pass  # let mitmproxy forward as-is
        elif path in BYBIT_BLOCK:
            _block(flow, f"Bybit endpoint blocked: {path}")
        else:
            _block(flow, f"Unknown Bybit endpoint: {path}")

    def _bybit_time_cap(self, flow: http.HTTPFlow, param_name: str):
        """Inject time cap into query params so response contains no future data."""
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        parsed = urlparse(flow.request.url)
        params = parse_qs(parsed.query, keep_blank_values=True)

        # Cap the end time parameter
        current_end = params.get(param_name, [None])[0]
        if current_end is None or int(current_end) > self.T:
            params[param_name] = [str(self.T)]

        new_query = urlencode(params, doseq=True)
        flow.request.url = urlunparse(parsed._replace(query=new_query))

    def _bybit_intercept_time(self, flow: http.HTTPFlow):
        """Return backtest time T instead of real server time."""
        if self.T is None:
            _block(flow, "Backtest time not set")
            return
        t_sec = self.T // 1000
        t_nano = str(self.T * 1_000_000)  # ms → ns
        _json_response(flow, {
            "retCode": 0, "retMsg": "OK",
            "result": {"timeSecond": str(t_sec), "timeNano": t_nano},
            "retExtInfo": {}, "time": self.T,
        })

    def _bybit_intercept_tickers(self, flow: http.HTTPFlow):
        """Construct ticker from kline + funding/history + open-interest at T."""
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

        try:
            # Fetch last kline candle at T
            kline_resp = self._client.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": category, "symbol": symbol, "interval": "1", "end": str(self.T), "limit": "1"},
            )
            kline_data = kline_resp.json()
            kline_list = kline_data.get("result", {}).get("list", [])

            last_price = "0"
            if kline_list:
                last_price = kline_list[0][4]  # close price

            # Fetch latest funding rate
            funding_resp = self._client.get(
                "https://api.bybit.com/v5/market/funding/history",
                params={"category": category, "symbol": symbol, "endTime": str(self.T), "limit": "1"},
            )
            funding_data = funding_resp.json()
            funding_list = funding_data.get("result", {}).get("list", [])
            funding_rate = funding_list[0]["fundingRate"] if funding_list else "0"

            # Fetch OI
            oi_resp = self._client.get(
                "https://api.bybit.com/v5/market/open-interest",
                params={"category": category, "symbol": symbol, "intervalTime": "5min", "endTime": str(self.T), "limit": "1"},
            )
            oi_data = oi_resp.json()
            oi_list = oi_data.get("result", {}).get("list", [])
            oi_value = oi_list[0]["openInterest"] if oi_list else "0"

            ticker = {
                "symbol": symbol,
                "lastPrice": last_price,
                "markPrice": last_price,
                "indexPrice": last_price,
                "prevPrice24h": last_price,
                "price24hPcnt": "0",
                "highPrice24h": last_price,
                "lowPrice24h": last_price,
                "turnover24h": "0",
                "volume24h": "0",
                "fundingRate": funding_rate,
                "nextFundingTime": "0",
                "openInterest": oi_value,
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
        except Exception as e:
            ctx.log.error(f"tickers intercept failed: {e}")
            _block(flow, f"tickers intercept failed: {e}")

    # -----------------------------------------------------------------------
    # HyperLiquid
    # -----------------------------------------------------------------------

    def _handle_hyperliquid(self, flow: http.HTTPFlow):
        if flow.request.method != "POST":
            _block(flow, "HyperLiquid only accepts POST")
            return

        try:
            body = json.loads(flow.request.content)
        except Exception:
            _block(flow, "Invalid JSON body")
            return

        req_type = body.get("type", "")

        if req_type in HL_TIME_CAP:
            self._hl_time_cap(flow, body, req_type)
        elif req_type == "allMids":
            self._hl_approximate_all_mids(flow, body)
        elif req_type in HL_FROM_LOCAL_FILE:
            self._hl_from_local_file(flow, body, req_type)
        elif req_type in HL_PASSTHROUGH:
            pass  # let mitmproxy forward as-is
        elif req_type in HL_BLOCK:
            _block(flow, f"HyperLiquid type blocked: {req_type}")
        else:
            _block(flow, f"Unknown HyperLiquid type: {req_type}")

    def _hl_time_cap(self, flow: http.HTTPFlow, body: dict, req_type: str):
        """Inject endTime=T into the request body."""
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        if req_type == "candleSnapshot":
            req = body.get("req", {})
            current_end = req.get("endTime")
            if current_end is None or int(current_end) > self.T:
                req["endTime"] = self.T
                body["req"] = req
        elif req_type == "fundingHistory":
            current_end = body.get("endTime")
            if current_end is None or int(current_end) > self.T:
                body["endTime"] = self.T

        flow.request.content = json.dumps(body).encode()

    def _hl_from_local_file(self, flow: http.HTTPFlow, body: dict, req_type: str):
        """Serve from pre-collected local snapshot file."""
        if self.data_dir is None:
            # Fallback: passthrough to real API
            ctx.log.warn(f"No data_dir set, passing through {req_type}")
            return

        file_path = self.data_dir / f"{req_type}.json"
        if not file_path.exists():
            # Fallback: passthrough
            ctx.log.warn(f"Snapshot not found: {file_path}, passing through")
            return

        data = json.loads(file_path.read_text())
        _json_response(flow, data)

    def _hl_approximate_all_mids(self, flow: http.HTTPFlow, body: dict):
        """Approximate allMids from candleSnapshot close prices at T."""
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        try:
            # Fetch meta to get asset list
            meta_resp = self._client.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "meta"},
            )
            meta = meta_resp.json()
            assets = [u["name"] for u in meta.get("universe", [])]

            mids = {}
            # Fetch close prices for top assets (batch to avoid too many requests)
            for asset in assets[:50]:
                try:
                    resp = self._client.post(
                        "https://api.hyperliquid.xyz/info",
                        json={
                            "type": "candleSnapshot",
                            "req": {
                                "coin": asset,
                                "interval": "1m",
                                "startTime": self.T - 60_000,
                                "endTime": self.T,
                            },
                        },
                    )
                    candles = resp.json()
                    if candles:
                        mids[asset] = candles[-1]["c"]
                except Exception:
                    continue

            _json_response(flow, mids)
        except Exception as e:
            ctx.log.error(f"allMids approximation failed: {e}")
            _block(flow, f"allMids approximation failed: {e}")


addons = [BacktestProxy()]
