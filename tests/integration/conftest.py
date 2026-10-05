"""Real loopback HTTP services and PostgreSQL; external network is denied."""

import asyncio
import importlib.util
import os
from pathlib import Path
import socket
import sys

import pytest
import uvicorn
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "hqg-dashboard/local"))

from server import LocalStore, create_app as dashboard_app
from control import new_account
from src.control.contract import APPROVED, IMAGE_DIGEST
from src.control.provider import Provider
from src.control.runtime import Runtime
from src.control.transport import Broker
from src.db.models import Base
from src.provider_instance.server import create_app as provider_app
from local.broker import create_app as broker_app
from local.readiness import create_app as readiness_app

ACCOUNT = "00000000-0000-4000-8000-000000000001"
ENGINE = "00000000-0000-4000-8000-000000000002"
SECRET = "FAKE-SYNC-SECRET"
WORKER_SECRET = "FAKE-WORKER-SECRET"


@pytest.fixture(autouse=True)
def deny_external_network(monkeypatch):
    original = socket.getaddrinfo

    def local_only(host, *args, **kwargs):
        if host not in ("127.0.0.1", "::1", "localhost", b"127.0.0.1"):
            raise AssertionError(f"External destination blocked: {host}")
        return original(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", local_only)
    for cls in (socket.socket,):
        original_connect = cls.connect
        original_connect_ex = cls.connect_ex

        def connect(sock, address):
            if isinstance(address, tuple) and address[0] not in ("127.0.0.1", "::1"):
                raise AssertionError(f"External socket blocked: {address}")
            return original_connect(sock, address)

        def connect_ex(sock, address):
            if isinstance(address, tuple) and address[0] not in ("127.0.0.1", "::1"):
                raise AssertionError(f"External socket blocked: {address}")
            return original_connect_ex(sock, address)

        monkeypatch.setattr(cls, "connect", connect)
        monkeypatch.setattr(cls, "connect_ex", connect_ex)


class HTTPServer:
    def __init__(self, app):
        self.app = app

    async def __aenter__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(128)
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        self.server = uvicorn.Server(
            uvicorn.Config(self.app, log_level="error", lifespan="on")
        )
        self.task = asyncio.create_task(self.server.serve(sockets=[self.sock]))
        for _ in range(200):
            if self.server.started:
                return self
            if self.task.done():
                await self.task
                raise RuntimeError("HTTP service failed startup")
            await asyncio.sleep(0.01)

        raise TimeoutError("Service did not start")

    async def __aexit__(self, *args):
        self.server.should_exit = True
        await self.task
        self.sock.close()


class Harness:
    def __init__(self, tmp_path):
        self.tmp_path = tmp_path

    async def __aenter__(self):
        from contextlib import AsyncExitStack

        self.stack = AsyncExitStack()

        try:
            return await self.setup()
        except BaseException:
            await self.__aexit__()
            raise

    async def setup(self):
        self.db = create_async_engine(os.environ["MVP_TEST_DATABASE_URL"])

        async with self.db.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)

        self.sessions = async_sessionmaker(self.db, expire_on_commit=False)
        self.runtime = Runtime(self.sessions, ACCOUNT)
        self.broker_app = broker_app()
        self.readiness_app = readiness_app()
        self.broker = await self.stack.enter_async_context(HTTPServer(self.broker_app))
        readiness = await self.stack.enter_async_context(HTTPServer(self.readiness_app))
        self.store = LocalStore(str(self.tmp_path / "dashboard.db"))

        account = new_account(
            ACCOUNT,
            ENGINE,
            {"operator": ["control", "live"], "paper-operator": ["control"]},
            APPROVED,
            IMAGE_DIGEST,
            SECRET,
        )
        self.dashboard_app = dashboard_app(
            self.store, account, readiness.url + "/control", readiness.url + "/"
        )
        self.dashboard_app.state.control.ttl = 1
        self.dashboard = await self.stack.enter_async_context(
            HTTPServer(self.dashboard_app)
        )
        self.provider = self.new_provider()
        self.provider_server = await self.stack.enter_async_context(
            HTTPServer(provider_app(self.provider, WORKER_SECRET, background=False))
        )
        import httpx

        self.user = httpx.AsyncClient(base_url=self.dashboard.url, trust_env=False)
        await self.user.post("/test/login", json={"actor": "operator"})
        self.execution = httpx.AsyncClient(
            base_url=self.provider_server.url,
            headers={"Authorization": "Bearer " + WORKER_SECRET},
            trust_env=False,
        )
        from uuid import uuid4

        self.worker_session = str(uuid4())
        await self.execution.post("/internal/sessions/" + self.worker_session)
        await self.provider.reconcile("paper")
        await self.provider.reconcile("live")
        assert await self.provider.sync_once()

        return self

    def new_provider(self):
        brokers = {
            m: Broker(
                m, self.broker.url + "/" + m, self.broker.url + "/" + m, timeout=0.3
            )
            for m in ("paper", "live")
        }

        return Provider(
            ACCOUNT,
            ENGINE,
            SECRET,
            self.dashboard.url,
            brokers,
            self.runtime,
            interval=0.1,
            timeout=0.25,
            ttl=1,
        )

    async def __aexit__(self, *args):
        if hasattr(self, "user"):
            await self.user.aclose()
        if hasattr(self, "execution"):
            await self.execution.aclose()

        await self.stack.aclose()
        await self.db.dispose()

    async def status(self):
        response = await self.user.get(f"/accounts/{ACCOUNT}/status")
        assert response.status_code == 200, response.text
        return response.json()

    async def configure(self, live=True):
        from uuid import uuid4

        state = await self.status()
        config = {
            "modes": {
                m: {
                    "portfolio_id": str(uuid4()),
                    "enabled": m == "paper" or live,
                    "strategies": (
                        [{**APPROVED[0], "allocation": 1}]
                        if m == "paper" or live
                        else []
                    ),
                }
                for m in ("paper", "live")
            }
        }
        body = {
            "configuration": config,
            "idempotency_key": str(uuid4()),
            "expected_control_version": state["control_version"],
        }
  
        response = await self.user.put(f"/accounts/{ACCOUNT}/configuration", json=body)
        assert response.status_code == 200, response.text
        await self.sync_clean()
        return body, response.json()

    async def sync_clean(self):
        assert await self.provider.sync_once()
        for mode in ("paper", "live"):
            if not self.provider.modes[mode]["gate_open"]:
                await self.provider.reconcile(mode)
        assert await self.provider.sync_once()

    async def action(self, action, mode=None, incident=None):
        from uuid import uuid4

        state = await self.status()
        body = {
            "action": action,
            "mode": mode,
            "expected_control_version": state["control_version"],
            "idempotency_key": str(uuid4()),
        }
        if mode:
            body["incident_id"] = (
                incident or state["observed"]["modes"][mode]["incident_id"]
            )

        response = await self.user.post(f"/accounts/{ACCOUNT}/actions", json=body)
        return body, response

    async def resume(self, mode):
        body, response = await self.action("resume", mode)
        assert response.status_code == 200, response.text
        await self.sync_clean()
        assert self.provider.modes[mode]["gate_open"]
        return body

    def context(self, mode):
        state = self.provider.state()
        return {
            **{
                k: state[k]
                for k in (
                    "account_id",
                    "boot_id",
                    "worker_session",
                    "control_version",
                    "configuration_digest",
                )
            },
            "mode": mode,
        }

    async def order(self, mode, operation_id=None):
        from uuid import uuid4

        return await self.execution.post(
            "/internal/order",
            json={
                "context": self.context(mode),
                "operation_id": operation_id or str(uuid4()),
                "symbol": "AAPL",
                "quantity": 1.0,
                "side": "buy",
            },
        )
