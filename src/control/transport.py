"""Local control transport and adapters for the existing Alpaca SDK clients."""

import asyncio
import math
import os
import threading
from datetime import date, datetime, timezone
from contextvars import ContextVar
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from alpaca.data.requests import StockLatestQuoteRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.requests import GetCalendarRequest

from src.control.cadence import MARKET_ZONE, completed_period

from src.execution_provider.alpaca import AlpacaExecutor
from src.marketdata_provider.alpaca import AlpacaMarketData


class OriginTransport(httpx.AsyncHTTPTransport):
    def __init__(self, origins):
        super().__init__(retries=0)
        self.origins = origins

    async def handle_async_request(self, request):
        origin = (
            request.url.scheme,
            request.url.host,
            request.url.port or (443 if request.url.scheme == "https" else 80),
        )
        if origin not in self.origins:
            raise ValueError("Destination outside configured origin")
        return await super().handle_async_request(request)


def bound_client(url, timeout=3, headers=None):
    parsed = urlsplit(url)
    origin = (
        parsed.scheme,
        parsed.hostname,
        parsed.port or (443 if parsed.scheme == "https" else 80),
    )
    return httpx.AsyncClient(
        base_url=url.rstrip("/") + "/",
        timeout=timeout,
        trust_env=False,
        follow_redirects=False,
        headers=headers,
        transport=OriginTransport({origin}),
    )


def dashboard_client(url, timeout=3, headers=None):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https" or not parsed.hostname or parsed.username
        or parsed.password or parsed.query or parsed.fragment
    ):
        raise ValueError(
            "An HTTPS dashboard URL without credentials, query or fragment is required"
        )
    return bound_client(url, timeout, headers)


def local_client(url, timeout=3, headers=None):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "http"
        or parsed.hostname
        not in {"127.0.0.1", "::1", "provider-api"}
        or not parsed.port
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Explicit local control HTTP URL required")
    return bound_client(url, timeout, headers)


# The SDK is synchronous. Its order admission hook runs after TLS/pool waits,
# immediately before urllib3 writes the request, preserving the provider gate.
_BEFORE_SEND = ContextVar("alpaca_before_send", default=None)


class AdmissionConnection(HTTPSConnection):
    def request(self, *args, **kwargs):
        guard = _BEFORE_SEND.get()
        if guard is not None:
            guard()
        return super().request(*args, **kwargs)


class AdmissionPool(HTTPSConnectionPool):
    ConnectionCls = AdmissionConnection


class SDKAdapter(HTTPAdapter):
    def __init__(self, timeout):
        self.timeout = timeout
        super().__init__(max_retries=0)

    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.pool_classes_by_scheme = {
            **self.poolmanager.pool_classes_by_scheme, "https": AdmissionPool,
        }

    def send(self, request, **kwargs):
        kwargs["timeout"] = kwargs.get("timeout") or self.timeout
        return super().send(request, **kwargs)


class Broker:
    """Adapt the existing Alpaca SDK clients to the gated execution interface."""

    def __init__(
        self, mode, api_key, secret_key, *, account_id=None, feed="iex", timeout=10
    ):
        if mode not in ("paper", "live"):
            raise ValueError("Explicit mode required")
        if not api_key or not secret_key:
            raise ValueError(f"Alpaca {mode} API key and secret are required")
        if feed not in {"iex", "sip"}:
            raise ValueError("ALPACA_DATA_FEED must be iex or sip")
        self.mode, self.feed = mode, feed
        self.account_id = str(UUID(account_id)) if account_id else None
        self.trading = AlpacaExecutor(
            api_key, secret_key, paper=mode == "paper"
        ).trading_client
        self.data = AlpacaMarketData(api_key, secret_key).data_client
        self.lock = threading.Lock()
        self.calendar, self.calendar_date = [], None
        self.bar_cache = {}
        for client in (self.trading, self.data):
            # No SDK resubmission of orders with uncertain outcomes. The provider
            # owns reconciliation and retries; SDK requests have bounded timeouts.
            client._retry = 0
            client._session.trust_env = False
            client._session.mount("https://", SDKAdapter(timeout))

    async def call(self, function, *args, before_send=None):
        loop = asyncio.get_running_loop()

        async def admit():
            before_send()

        def invoke():
            guard = (
                lambda: asyncio.run_coroutine_threadsafe(admit(), loop).result()
            ) if before_send else None
            with self.lock:
                token = _BEFORE_SEND.set(guard)
                try:
                    return function(*args)
                finally:
                    _BEFORE_SEND.reset(token)

        return await asyncio.to_thread(invoke)

    async def request(self, method, path, *, params=None, json=None, before_send=None):
        if method not in {"GET", "POST", "DELETE"} or not path.startswith("v2/"):
            raise ValueError("Unsupported broker request")
        return await self.call(
            getattr(self.trading, method.lower()),
            "/" + path[3:],
            json if method == "POST" else params,
            before_send=before_send,
        )

    async def quotes(self, symbols):
        if not symbols:
            return {}
        quotes = await self.call(
            self.data.get_stock_latest_quote,
            StockLatestQuoteRequest(symbol_or_symbols=symbols, feed=self.feed),
        )
        prices = {symbol: float(quotes[symbol].ask_price) for symbol in symbols}
        if any(not math.isfinite(price) or price <= 0 for price in prices.values()):
            raise ValueError("Alpaca returned an invalid or unavailable quote")
        return prices

    async def bars(self, symbols, bar_size="1d", execution="close_to_close"):
        now = datetime.now(timezone.utc)
        today = now.astimezone(MARKET_ZONE).date()
        if self.calendar_date != today:
            calendar = await self.call(
                self.trading.get_calendar,
                GetCalendarRequest(start=date(today.year - 1, 1, 1), end=date(today.year + 1, 12, 31)),
            )
            if not calendar:
                raise ValueError("Alpaca returned an empty market calendar")
            self.calendar, self.calendar_date = calendar, today
        period = completed_period(self.calendar, bar_size, execution, now)
        if period is None:
            return {}
        start, end = period
        cached_start, cached = self.bar_cache.get(bar_size, (None, {}))
        if cached_start != start:
            cached = {}
        missing = [symbol for symbol in symbols if symbol not in cached]
        if not missing:
            return {symbol: cached[symbol] for symbol in symbols}
        timeframe = {
            "1d": TimeFrame.Day, "1w": TimeFrame.Week, "1m": TimeFrame.Month,
            "1q": TimeFrame(3, TimeFrameUnit.Month),
        }[bar_size]
        bars = await self.call(
            self.data.get_stock_bars,
            StockBarsRequest(
                symbol_or_symbols=missing, feed=self.feed, timeframe=timeframe,
                start=start, end=end,
            ),
        )
        # Missing/delayed symbols are retried; never mix periods in a strategy Slice.
        if any(
            symbol not in bars.data or len(bars[symbol]) != 1
            or bars[symbol][0].timestamp != start
            for symbol in missing
        ):
            return {}
        result = {
            symbol: {
                key: float(getattr(bars[symbol][0], key))
                for key in ("open", "high", "low", "close", "volume")
            }
            for symbol in missing
        }
        if any(
            not math.isfinite(value) or value < 0 or (key != "volume" and value == 0)
            for bar in result.values() for key, value in bar.items()
        ):
            raise ValueError("Alpaca returned invalid bars")
        cached.update({
            symbol: {**bar, "timestamp": start.isoformat()} for symbol, bar in result.items()
        })
        self.bar_cache[bar_size] = (start, cached)
        return {symbol: cached[symbol] for symbol in symbols}

    async def close(self):
        for client in (self.trading, self.data):
            await self.call(client._session.close)


def brokers_from_environment():
    """Use the existing key pair and ALPACA_PAPER mode selector."""
    key = os.environ.get("ALPACA_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY")
    if not key or not secret:
        raise ValueError("Both ALPACA_API_KEY and ALPACA_SECRET_KEY are required")
    paper = os.environ.get("ALPACA_PAPER", "").strip().lower() or "true"
    if paper not in {"true", "false"}:
        raise ValueError("ALPACA_PAPER must be true or false")
    mode = "paper" if paper == "true" else "live"
    feed = os.environ.get("ALPACA_DATA_FEED", "iex")
    return {mode: Broker(mode, key, secret, feed=feed)}


class BrokerReader:
    """Worker read access through the authenticated provider; no Alpaca credentials."""

    def __init__(self, mode, client):
        self.mode, self.client = mode, client

    async def get(self, resource, **params):
        response = await self.client.get(
            f"internal/broker/{self.mode}/{resource}", params=params
        )
        response.raise_for_status()
        return response.json()

    async def request(self, method, path, **kwargs):
        if (
            method != "GET"
            or path not in {"v2/account", "v2/positions"}
            or set(kwargs) - {"params"}
        ):
            raise ValueError("Worker broker access is read-only")
        return await self.get(path.removeprefix("v2/"), **kwargs.get("params", {}))

    async def quotes(self, symbols):
        return await self.get("quotes", symbols=",".join(symbols)) if symbols else {}

    async def bars(self, symbols, bar_size="1d", execution="close_to_close"):
        return await self.get(
            "bars", symbols=",".join(symbols), bar_size=bar_size, execution=execution
        )

    async def close(self):
        pass  # The worker owns the shared client.
