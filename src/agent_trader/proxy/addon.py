"""
mitmproxy addon for backtesting. Intercepts HTTP traffic from the agent sandbox
and enforces time isolation: the agent cannot see data after backtest time T.

47 endpoints: Bybit (22) + HyperLiquid (25). See docs/mitm.md.

Usage:
    mitmdump --listen-port 8080 -s src/agent_trader/proxy/addon.py --set data_dir=data/proxy_snapshots
"""

import asyncio
import json
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from mitmproxy import ctx, http

from agent_trader.data.market import fetch_candles

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

HL_TIME_CAP = {"fundingHistory"}

HL_INTERCEPT = {"candleSnapshot", "allMids", "metaAndAssetCtxs", "spotMetaAndAssetCtxs"}

HL_FROM_LOCAL_FILE = {"meta", "allPerpMetas", "spotMeta"}

HL_PASSTHROUGH = {"perpCategories", "perpAnnotation", "perpConciseAnnotations"}

HL_BLOCK = {
    "l2Book", "predictedFundings", "perpsAtOpenInterestCap",
    "perpDexs", "perpDeployAuctionStatus", "perpDexLimits", "perpDexStatus",
    "spotPairDeployAuctionStatus",
    "vaultDetails", "tokenDetails", "borrowLendReserveState",
    "allBorrowLendReserveStates", "alignedQuoteTokenInfo", "outcomeMeta",
}

FETCH_SEMAPHORE = asyncio.Semaphore(50)


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
        self._client: httpx.AsyncClient | None = None

    def load(self, loader):
        loader.add_option("data_dir", str, "", "Path to pre-collected proxy snapshots")

    def configure(self, updates):
        if "data_dir" in updates:
            d = ctx.options.data_dir
            if d:
                self.data_dir = Path(d)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=15)
        return self._client

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
        elif path == "/v5/market/tickers":
            await self._bybit_intercept_tickers(flow)
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

    async def _bybit_intercept_tickers(self, flow: http.HTTPFlow):
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
            client = await self._get_client()

            kline_resp, funding_resp, oi_resp = await asyncio.gather(
                client.get("https://api.bybit.com/v5/market/kline", params={
                    "category": category, "symbol": symbol, "interval": "1",
                    "end": str(self.T), "limit": "1",
                }),
                client.get("https://api.bybit.com/v5/market/funding/history", params={
                    "category": category, "symbol": symbol,
                    "endTime": str(self.T), "limit": "1",
                }),
                client.get("https://api.bybit.com/v5/market/open-interest", params={
                    "category": category, "symbol": symbol,
                    "intervalTime": "5min", "endTime": str(self.T), "limit": "1",
                }),
            )

            kline_list = kline_resp.json().get("result", {}).get("list", [])
            last_price = kline_list[0][4] if kline_list else "0"

            funding_list = funding_resp.json().get("result", {}).get("list", [])
            funding_rate = funding_list[0]["fundingRate"] if funding_list else "0"

            oi_list = oi_resp.json().get("result", {}).get("list", [])
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

        if req_type in HL_TIME_CAP:
            self._hl_time_cap(flow, body, req_type)
        elif req_type in HL_INTERCEPT:
            await self._hl_intercept(flow, body, req_type)
        elif req_type in HL_FROM_LOCAL_FILE:
            self._hl_from_local_file(flow, body, req_type)
        elif req_type in HL_PASSTHROUGH:
            pass
        elif req_type in HL_BLOCK:
            _block(flow, f"HyperLiquid type blocked: {req_type}")
        else:
            _block(flow, f"Unknown HyperLiquid type: {req_type}")

    def _hl_time_cap(self, flow: http.HTTPFlow, body: dict, req_type: str):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        if req_type == "fundingHistory":
            current_end = body.get("endTime")
            if current_end is None or int(current_end) > self.T:
                body["endTime"] = self.T

        flow.request.content = json.dumps(body).encode()

    async def _hl_intercept(self, flow: http.HTTPFlow, body: dict, req_type: str):
        if self.T is None:
            _block(flow, "Backtest time not set")
            return

        if req_type == "candleSnapshot":
            await self._hl_intercept_candles(flow, body)
        elif req_type == "allMids":
            await self._hl_intercept_all_mids(flow)
        elif req_type in ("metaAndAssetCtxs", "spotMetaAndAssetCtxs"):
            await self._hl_intercept_meta_and_ctxs(flow, body, req_type)

    async def _hl_intercept_candles(self, flow: http.HTTPFlow, body: dict):

        req = body.get("req", {})
        coin = req.get("coin", "")
        interval = req.get("interval", "1m")
        start_time = int(req.get("startTime", 0))
        end_time = min(int(req.get("endTime", self.T)), self.T)

        candles = await fetch_candles(coin, start_time, end_time, interval)

        hl_format = [
            {"t": c.timestamp_ms, "o": str(c.open), "h": str(c.high),
             "l": str(c.low), "c": str(c.close), "v": str(c.volume)}
            for c in candles
        ]
        _json_response(flow, hl_format)

    async def _hl_intercept_all_mids(self, flow: http.HTTPFlow):

        if not self.data_dir or not (self.data_dir / "allPerpMetas.json").exists():
            _block(flow, "allPerpMetas.json not found. Run: python scripts/collect_snapshots.py")
            return

        all_metas = json.loads((self.data_dir / "allPerpMetas.json").read_text())
        assets = []
        for group in all_metas:
            for u in group.get("universe", []):
                assets.append(u["name"])

        async def _get_mid(asset: str) -> tuple[str, str | None]:
            async with FETCH_SEMAPHORE:
                try:
                    candles = await fetch_candles(asset, self.T - 60_000, self.T, "1m")
                    if candles:
                        return asset, candles[-1].close
                except Exception:
                    pass
                return asset, None

        results = await asyncio.gather(*[_get_mid(a) for a in assets])
        mids = {asset: str(price) for asset, price in results if price is not None}
        _json_response(flow, mids)

    async def _hl_intercept_meta_and_ctxs(self, flow: http.HTTPFlow, body: dict, req_type: str):

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

        async def _get_ctx(asset: str) -> dict:
            async with FETCH_SEMAPHORE:
                mark_price = None
                funding = "0"
                open_interest = "0"
                try:
                    candles = await fetch_candles(asset, self.T - 60_000, self.T, "1m")
                    if candles:
                        mark_price = _fmt(candles[-1].close)
                except Exception:
                    pass

                if mark_price is None:
                    mark_price = "0"

                try:
                    client = await self._get_client()
                    resp = await client.post("https://api.hyperliquid.xyz/info", json={
                        "type": "fundingHistory", "coin": asset,
                        "startTime": self.T - 8 * 3600_000, "endTime": self.T,
                    })
                    fh = resp.json()
                    if fh:
                        funding = fh[-1].get("fundingRate", "0")
                except Exception:
                    pass

                return {
                    "funding": funding,
                    "openInterest": open_interest,
                    "prevDayPx": mark_price,
                    "dayNtlVlm": "0",
                    "premium": "0",
                    "oraclePx": mark_price,
                    "markPx": mark_price,
                    "midPx": mark_price,
                    "impactPxs": [mark_price, mark_price],
                }

        ctxs = await asyncio.wait_for(
            asyncio.gather(*[_get_ctx(a) for a in assets]),
            timeout=60,
        )
        _json_response(flow, [meta, list(ctxs)])

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
