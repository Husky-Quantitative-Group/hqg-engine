import asyncio
import json
import threading
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import requests
from alpaca.common.exceptions import APIError
from fastapi import HTTPException
from urllib3.connection import HTTPSConnection

from src.control.transport import (
    AdmissionConnection, AdmissionPool, Broker, BrokerReader, brokers_from_environment,
)


def response(request, body, status=200):
    result = requests.Response()
    result.status_code, result.request = status, request
    result._content = json.dumps(body).encode()
    return result


def test_existing_sdk_clients_supply_real_hosts_credentials_feed_and_bars(monkeypatch):
    seen = []

    def send(self, request, **kwargs):
        seen.append(request)
        path = urlsplit(request.url).path
        if path == "/v2/account":
            return response(request, {"id": "account"})
        if path.endswith("quotes/latest"):
            return response(request, {"quotes": {"SPY": {
                "ap": 500, "as": 10, "bp": 499, "bs": 10,
                "ax": "V", "bx": "V", "t": "2026-10-09T19:59:00Z",
            }}})
        return response(request, {"bars": {"SPY": {
            "o": 490, "h": 510, "l": 485, "c": 500, "v": 1000000, "n": 10, "vw": 500,
            "t": "2026-10-09T19:59:00Z",
        }}})

    monkeypatch.setattr(requests.Session, "send", send)

    async def scenario():
        broker = Broker("paper", "test-key", "test-secret")
        try:
            assert await broker.request("GET", "v2/account") == {"id": "account"}
            assert await broker.quotes(["SPY"]) == {"SPY": 500}
            assert (await broker.bars(["SPY"]))["SPY"] == {
                "open": 490, "high": 510, "low": 485,
                "close": 500, "volume": 1000000,
            }
            assert [urlsplit(r.url).hostname for r in seen] == [
                "paper-api.alpaca.markets", "data.alpaca.markets", "data.alpaca.markets",
            ]
            assert all(r.headers["APCA-API-KEY-ID"] == "test-key" for r in seen)
            assert all(r.headers["APCA-API-SECRET-KEY"] == "test-secret" for r in seen)
            assert urlsplit(seen[-1].url).path == "/v2/stocks/bars/latest"
            assert parse_qs(urlsplit(seen[-1].url).query)["feed"] == ["iex"]
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
