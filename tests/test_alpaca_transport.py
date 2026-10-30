import asyncio
import json
import threading
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import requests
from alpaca.common.exceptions import APIError
from fastapi import HTTPException
from urllib3.connection import HTTPSConnection
from urllib3.exceptions import ConnectTimeoutError, SSLError
from unittest.mock import Mock

from src.control.transport import (
    AdmissionConnection, AdmissionPool, Broker, BrokerReader, brokers_from_environment,
    OrderNotSubmitted,
)
from src.control import transport


def response(request, body, status=200):
    result = requests.Response()
    result.status_code, result.request = status, request
    result._content = json.dumps(body).encode()
    return result


@pytest.mark.parametrize("bar_size,timeframe,start", [
    ("1d", "1Day", "2026-10-02T00:00:00-04:00"),
    ("1w", "1Week", "2026-09-28T00:00:00-04:00"),
    ("1m", "1Month", "2026-09-01T00:00:00-04:00"),
    ("1q", "3Month", "2026-07-01T00:00:00-04:00"),
])
def test_existing_sdk_clients_supply_real_hosts_credentials_feed_and_bars(monkeypatch, bar_size, timeframe, start):
    seen = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 2, 20, 2, tzinfo=timezone.utc)

    monkeypatch.setattr(transport, "datetime", Clock)

    def send(self, request, **kwargs):
        seen.append(request)
        path = urlsplit(request.url).path
        if path == "/v2/account":
            return response(request, {"id": "account"})
        if path == "/v2/calendar":
            return response(request, [
                {"date": day, "open": "09:30", "close": "16:00"}
                for day in ("2026-09-28", "2026-09-30", "2026-10-01", "2026-10-02", "2026-10-05")
            ])
        if path.endswith("quotes/latest"):
            return response(request, {"quotes": {"SPY": {
                "ap": 500, "as": 10, "bp": 499, "bs": 10,
                "ax": "V", "bx": "V", "t": "2026-10-09T19:59:00Z",
            }}})
        return response(request, {"bars": {"SPY": [{
            "o": 490, "h": 510, "l": 485, "c": 500, "v": 1000000, "n": 10, "vw": 500,
            "t": start,
        }]}})

    monkeypatch.setattr(requests.Session, "send", send)

    async def scenario():
        broker = Broker("paper", "test-key", "test-secret")
        try:
            assert await broker.request("GET", "v2/account") == {"id": "account"}
            assert await broker.quotes(["SPY"]) == {"SPY": 500}
            assert (await broker.bars(["SPY"], bar_size))["SPY"] == {
                "open": 490, "high": 510, "low": 485,
                "close": 500, "volume": 1000000,
                "timestamp": start,
            }
            assert [urlsplit(r.url).hostname for r in seen] == [
                "paper-api.alpaca.markets", "data.alpaca.markets",
                "paper-api.alpaca.markets", "data.alpaca.markets",
            ]
            assert all(r.headers["APCA-API-KEY-ID"] == "test-key" for r in seen)
            assert all(r.headers["APCA-API-SECRET-KEY"] == "test-secret" for r in seen)
            assert urlsplit(seen[-1].url).path == "/v2/stocks/bars"
            assert parse_qs(urlsplit(seen[-1].url).query)["feed"] == ["iex"]
            assert parse_qs(urlsplit(seen[-1].url).query)["timeframe"] == [timeframe]
            assert parse_qs(urlsplit(seen[-1].url).query)["start"] == [
                datetime.fromisoformat(start).astimezone(timezone.utc).isoformat()
            ]
            await broker.bars(["SPY"], bar_size)
            assert len(seen) == 4  # Polling an already completed bar uses the cache.
        finally:
            await broker.close()

    asyncio.run(scenario())


def test_completed_bars_wait_for_all_symbols_and_refresh_at_the_next_close(monkeypatch):
    now = datetime(2026, 10, 2, 20, 2, tzinfo=timezone.utc)
    timestamp, missing, reads = "2026-10-01T04:00:00Z", False, []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    def send(self, request, **kwargs):
        if urlsplit(request.url).path == "/v2/calendar":
            return response(request, [
                {"date": day, "open": "09:30", "close": "16:00"}
                for day in ("2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06")
            ])
        reads.append(request)
        return response(request, {"bars": {
            symbol: [{"t": timestamp, "o": 98, "h": 103, "l": 96, "c": 100,
                      "v": 12345, "n": 10, "vw": 100}]
            for symbol in ("AAPL", "MSFT") if not missing or symbol != "MSFT"
        }})

    monkeypatch.setattr(transport, "datetime", Clock)
    monkeypatch.setattr(requests.Session, "send", send)

    async def scenario():
        nonlocal now, timestamp, missing
        broker = Broker("paper", "key", "secret")
        try:
            assert await broker.bars(["AAPL", "MSFT"]) == {}  # Older period.
            timestamp, missing = "2026-10-02T04:00:00Z", True
            assert await broker.bars(["AAPL", "MSFT"]) == {}  # Incomplete universe.
            missing = False
            assert len(await broker.bars(["AAPL", "MSFT"])) == 2
            await broker.bars(["AAPL", "MSFT"])
            assert len(reads) == 3
            # Monday's in-progress daily bar is not eligible before its close.
            now = datetime(2026, 10, 5, 19, 0, tzinfo=timezone.utc)
            await broker.bars(["AAPL", "MSFT"])
            assert len(reads) == 3
            now = datetime(2026, 10, 5, 20, 2, tzinfo=timezone.utc)
            timestamp = "2026-10-05T04:00:00Z"
            result = await broker.bars(["AAPL", "MSFT"])
            assert result["AAPL"]["timestamp"] == "2026-10-05T00:00:00-04:00"
            assert len(reads) == 4
        finally:
            await broker.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("paper,mode,host", [
    (None, "paper", "paper-api.alpaca.markets"),
    ("", "paper", "paper-api.alpaca.markets"),
    ("true", "paper", "paper-api.alpaca.markets"),
    (" TRUE ", "paper", "paper-api.alpaca.markets"),
    ("false", "live", "api.alpaca.markets"),
    (" FALSE ", "live", "api.alpaca.markets"),
])
def test_existing_environment_names_select_account_mode(monkeypatch, paper, mode, host):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    monkeypatch.delenv("ALPACA_DATA_FEED", raising=False)
    if paper is None:
        monkeypatch.delenv("ALPACA_PAPER", raising=False)
    else:
        monkeypatch.setenv("ALPACA_PAPER", paper)

    async def scenario():
        brokers = brokers_from_environment()
        assert set(brokers) == {mode}
        broker = brokers[mode]
        try:
            assert broker.trading._base_url == "https://" + host
            assert broker.trading._api_key == broker.data._api_key == "test-key"
            assert broker.trading._secret_key == broker.data._secret_key == "test-secret"
        finally:
            await broker.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("key,secret", [(None, None), ("test-key", None), (None, "test-secret")])
def test_environment_requires_complete_existing_key_pair(monkeypatch, key, secret):
    for name, value in (("ALPACA_API_KEY", key), ("ALPACA_SECRET_KEY", secret)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="Both ALPACA_API_KEY and ALPACA_SECRET_KEY"):
        brokers_from_environment()


def test_environment_rejects_invalid_paper_selector(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    monkeypatch.setenv("ALPACA_PAPER", "flase")
    with pytest.raises(ValueError, match="ALPACA_PAPER must be true or false"):
        brokers_from_environment()


@pytest.mark.parametrize("proxy,connections", [
    (None, [("paper-api.alpaca.markets", None), ("data.alpaca.markets", None)]),
    ("", [("paper-api.alpaca.markets", None), ("data.alpaca.markets", None)]),
    (" http://proxy.test:3128 ", [
        ("proxy.test", "paper-api.alpaca.markets"), ("proxy.test", "data.alpaca.markets"),
    ]),
])
def test_environment_proxy_is_the_only_proxy_both_clients_use(monkeypatch, proxy, connections):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    monkeypatch.delenv("ALPACA_PAPER", raising=False)
    monkeypatch.delenv("ALPACA_DATA_FEED", raising=False)
    monkeypatch.setenv("https_proxy", "http://environment-proxy.test:3128")
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    if proxy is None:
        monkeypatch.delenv("ALPACA_PROXY_URL", raising=False)
    else:
        monkeypatch.setenv("ALPACA_PROXY_URL", proxy)
    seen = []

    def connect(self):
        # The host connected to, and the host tunnelled to through it, if any.
        seen.append((self.host, self._tunnel_host))
        raise ConnectionRefusedError("Injected connection refusal")

    monkeypatch.setattr(HTTPSConnection, "connect", connect)

    async def scenario():
        broker = brokers_from_environment()["paper"]
        try:
            with pytest.raises(requests.RequestException):
                await broker.request("GET", "v2/account")
            with pytest.raises(requests.RequestException):
                await broker.quotes(["SPY"])
            assert seen == connections
        finally:
            await broker.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("proxy", ["proxy.test:3128", "socks5://proxy.test:1080", "http://:3128"])
def test_environment_rejects_invalid_proxy(monkeypatch, proxy):
    monkeypatch.setenv("ALPACA_API_KEY", "test-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret")
    monkeypatch.delenv("ALPACA_PAPER", raising=False)
    monkeypatch.setenv("ALPACA_PROXY_URL", proxy)
    with pytest.raises(ValueError, match="ALPACA_PROXY_URL must be an http or https proxy URL"):
        brokers_from_environment()


def test_sdk_does_not_retry_orders_after_uncertain_response(monkeypatch):
    seen = []

    def send(self, request, **kwargs):
        seen.append(request)
        return response(request, {"message": "Gateway timeout", "code": 504}, 504)

    monkeypatch.setattr(requests.Session, "send", send)

    async def scenario():
        broker = Broker("paper", "key", "secret")
        try:
            with pytest.raises(APIError) as exc:
                await broker.request("POST", "v2/orders", json={"client_order_id": "test-order"})
            assert exc.value.status_code == 504
            assert len(seen) == 1
        finally:
            await broker.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("proxy", [None, "http://proxy.test:3128"])
@pytest.mark.parametrize("failure", [
    ConnectionRefusedError("Injected connection refusal"),
    ConnectTimeoutError("Injected connection timeout"),
    SSLError("Injected TLS failure"),
])
def test_sdk_identifies_order_failures_before_transmission(monkeypatch, failure, proxy):
    def connect(self):
        raise failure

    monkeypatch.setattr(HTTPSConnection, "connect", connect)

    async def scenario():
        broker, admit = Broker("paper", "key", "secret", proxy=proxy), Mock()
        try:
            with pytest.raises(OrderNotSubmitted):
                await broker.request("POST", "v2/orders", json={}, before_send=admit)
            admit.assert_not_called()
            # Read failures remain ordinary transport errors.
            with pytest.raises(requests.RequestException):
                await broker.request("GET", "v2/account")
        finally:
            await broker.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [requests.ReadTimeout, requests.ConnectionError])
def test_sdk_keeps_failures_after_submission_begins_uncertain(monkeypatch, failure):
    monkeypatch.setattr(AdmissionPool, "_validate_conn", lambda *args: None)

    def write(self, *args, **kwargs):
        raise failure("Injected failure after admission")

    monkeypatch.setattr(HTTPSConnection, "request", write)

    async def scenario():
        broker, admit = Broker("paper", "key", "secret"), Mock()
        try:
            with pytest.raises(requests.RequestException):
                await broker.request("POST", "v2/orders", json={}, before_send=admit)
            admit.assert_called_once_with()
        finally:
            await broker.close()

    asyncio.run(scenario())


def test_sdk_orders_through_a_proxy_keep_the_admission_check(monkeypatch):
    monkeypatch.setattr(AdmissionPool, "_validate_conn", lambda *args: None)
    tunnels = []

    def connect(self):
        tunnels.append((self.host, self._tunnel_host))

    def write(self, *args, **kwargs):
        raise requests.ConnectionError("Injected failure after admission")

    monkeypatch.setattr(HTTPSConnection, "connect", connect)
    monkeypatch.setattr(HTTPSConnection, "request", write)

    async def scenario():
        broker = Broker("paper", "key", "secret", proxy="http://proxy.test:3128")
        admit = Mock()
        try:
            # Admitted, so the failure is uncertain rather than OrderNotSubmitted.
            with pytest.raises(requests.RequestException):
                await broker.request("POST", "v2/orders", json={}, before_send=admit)
            admit.assert_called_once_with()
            assert tunnels == [("proxy.test", "paper-api.alpaca.markets")]
        finally:
            await broker.close()

    asyncio.run(scenario())


def test_sdk_rechecks_admission_after_connection_wait_without_blocking_loop(monkeypatch):
    waiting, release = threading.Event(), threading.Event()
    wrote = []
    pool = AdmissionPool("paper-api.alpaca.markets")
    conn = AdmissionConnection("paper-api.alpaca.markets")

    def validate(connection):
        waiting.set()
        assert release.wait(5)

    def write(self, *args, **kwargs):
        wrote.append(True)
        raise AssertionError("An expired order reached the wire")

    monkeypatch.setattr(pool, "_validate_conn", validate)
    monkeypatch.setattr(HTTPSConnection, "request", write)

    async def scenario():
        broker = Broker("paper", "key", "secret")
        allowed = True

        def admit():
            if not allowed:
                raise HTTPException(409, "Permission expired")

        try:
            assert broker.trading._session.get_adapter("https://").poolmanager.pool_classes_by_scheme["https"] is AdmissionPool
            task = asyncio.create_task(broker.call(
                pool._make_request, conn, "POST", "/v2/orders", before_send=admit,
            ))
            assert await asyncio.to_thread(waiting.wait, 5)
            allowed = False
            release.set()
            with pytest.raises(HTTPException) as exc:
                await task
            assert exc.value.status_code == 409
            assert not wrote
        finally:
            release.set()
            await broker.close()
            conn.close()
            pool.close()

    asyncio.run(scenario())


def test_worker_reader_rejects_mutation_and_uses_only_provider_auth():
    async def scenario():
        seen = []

        def respond(request):
            seen.append(request)
            return httpx.Response(200, json={"SPY": 500})

        async with httpx.AsyncClient(
            base_url="http://127.0.0.1:8013",
            headers={"Authorization": "Bearer worker-secret"},
            transport=httpx.MockTransport(respond),
        ) as client:
            reader = BrokerReader("paper", client)
            assert await reader.quotes(["SPY"]) == {"SPY": 500}
            with pytest.raises(ValueError, match="read-only"):
                await reader.request("POST", "v2/orders", json={})
        assert len(seen) == 1
        assert seen[0].url.path == "/internal/broker/paper/quotes"
        assert "APCA-API-KEY-ID" not in seen[0].headers

    asyncio.run(scenario())
