import asyncio
import os
from uuid import uuid4

import httpx
import pytest

from conftest import Harness, ACCOUNT, WORKER_SECRET
from src.control.contract import APPROVED
from src.control.sandbox import Sandbox
from src.control.transport import Broker
from src.control.worker import Worker

SOURCE = """from hqg_algorithms import Strategy, TargetWeights
from helper import TARGET

class Custom(Strategy):
    universe = ["AAPL", "MSFT"]

    def __init__(self):
        self.cycles = 0

    def on_data(self, data, portfolio):
        self.cycles += 1
        return TargetWeights({"AAPL": TARGET if self.cycles == 1 else 0.3})
"""


async def saved_strategy(h, source=SOURCE):
    response = await h.user.post(
        "/strategies",
        json={
            "source_strategy_id": "1",
            "name": "Dashboard custom",
            "readme_content": "# Custom",
        },
    )
    assert response.status_code == 200, response.text
    metadata = response.json()
    response = await h.user.post(
        f"/strategies/{metadata['id']}/artifacts",
        json={
            "files": [
                {"artifactId": "main.py", "content": source},
                {"artifactId": "helper.py", "content": "TARGET = 0.6"},
            ]
        },
    )
    assert response.status_code == 200, response.text
    return metadata["id"], response.json()["version"]


async def register(h, strategy_id, version):
    state = await h.status()
    body = {
        "strategy_id": strategy_id,
        "version": version,
        "idempotency_key": str(uuid4()),
        "expected_control_version": state["control_version"],
    }
    response = await h.user.post(f"/accounts/{ACCOUNT}/strategies", json=body)
    return body, response


async def configure(h, strategies):
    state = await h.status()
    config = {
        "modes": {
            m: {"portfolio_id": str(uuid4()), "enabled": True,
                "strategies": strategies}
            for m in ("paper", "live")
        }
    }
    response = await h.user.put(
        f"/accounts/{ACCOUNT}/configuration",
        json={
            "configuration": config,
            "idempotency_key": str(uuid4()),
            "expected_control_version": state["control_version"],
        },
    )
    assert response.status_code == 200, response.text
    await h.sync_clean()


def new_worker(h):
    return Worker(
        h.provider_server.url,
        WORKER_SECRET,
        {
            m: Broker(m, h.broker.url + "/" + m,
                      h.broker.url + "/" + m, read_only=True)
            for m in ("paper", "live")
        },
    )


async def close_worker(worker):
    await worker.close_strategies()
    await worker.client.aclose()
    for broker in worker.brokers.values():
        await broker.close()


def test_registration_permissions_pinning_retries_and_pause(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            async with httpx.AsyncClient(
                base_url=h.dashboard.url, trust_env=False
            ) as stranger:
                assert (await stranger.get("/strategies")).status_code == 403
                assert (
                    await stranger.post(f"/accounts/{ACCOUNT}/strategies", json={})
                ).status_code == 403
            strategy_id, version = await saved_strategy(h)
            assert version == 2
            body, response = await register(h, strategy_id, version)
            assert response.status_code == 200, response.text
            assert (
                await h.user.post(f"/accounts/{ACCOUNT}/strategies", json=body)
            ).json() == response.json()
            identity = (await h.status())["approved_strategies"][-1]
            installed = h.store.get(
                ACCOUNT, "package#" + identity["source_digest"])
            assert installed["files"]["helper.py"] == "TARGET = 0.6"
            assert (
                await h.user.post(
                    f"/strategies/{strategy_id}/artifacts",
                    json={
                        "files": [
                            {
                                "artifactId": "main.py",
                                "content": SOURCE.replace("0.3", "0.2"),
                            },
                        ]
                    },
                )
            ).json()["version"] == 3
            assert (
                h.dashboard_app.state.control.strategy_source(strategy_id, 3)["files"][
                    "helper.py"
                ]
                == "TARGET = 0.6"
            )
            assert (
                h.store.get(ACCOUNT, "package#" + identity["source_digest"])
                == installed
            )
            assert (await register(h, strategy_id, 999))[1].status_code == 400
            # Two-phase acknowledgement: desired registration first, then provider receipt.
            await h.sync_clean()
            assert identity in (await h.status())["observed"]["approved_strategies"]
            await configure(h, [{**identity, "allocation": 1}])
            await h.resume("paper")
            assert (await register(h, strategy_id, 3))[1].status_code == 409
            assert (await h.action("stop_all"))[1].status_code == 200
            await h.sync_clean()
            assert (await register(h, strategy_id, 3))[1].status_code == 200
            await h.sync_clean()
            assert (
                h.provider.configuration["modes"]["paper"]["strategies"][0]["version"]
                == 2
            )
            # Tampered package content fails closed before being installed.
            original_post = h.provider.dashboard.post

            async def tampered(*args, **kwargs):
                result = await original_post(*args, **kwargs)
                data = result.json()
                data["strategy_packages"][identity["source_digest"]]["files"][
                    "main.py"
                ] += "\n# tampered"
                return httpx.Response(200, json=data, request=result.request)

            h.provider.dashboard.post = tampered
            assert not await h.provider.sync_once()
            assert "digest mismatch" in h.provider.sync_error

    asyncio.run(scenario())


requires_sandbox = pytest.mark.skipif(
    not os.environ.get("STRATEGY_SANDBOX_IMAGE"),
    reason="Build/set STRATEGY_SANDBOX_IMAGE for Docker isolation checks",
)


@requires_sandbox
def test_dashboard_package_runs_both_modes_and_multiple_allocations(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            h.provider.max_ttl = h.dashboard_app.state.control.ttl = 15
            strategy_id, version = await saved_strategy(h)
            assert (await register(h, strategy_id, version))[1].status_code == 200
            await h.sync_clean()
            identity = (await h.status())["approved_strategies"][-1]
            worker = new_worker(h)
            try:
                await worker.register()
                await h.sync_clean()
                await configure(
                    h,
                    [
                        {**identity, "allocation": 0.5},
                        {**APPROVED[0], "allocation": 0.5},
                    ],
                )
                await h.resume("paper")
                await h.resume("live")
                await asyncio.gather(worker.cycle("paper"), worker.cycle("live"))
                assert h.broker_app.state.contexts["paper"]["positions"] == {
                    "AAPL": 50.0,
                    "MSFT": 10.0,
                }
                assert h.broker_app.state.contexts["live"]["positions"] == {
                    "AAPL": 100.0,
                    "MSFT": 20.0,
                }
                await h.provider.sync_once()  # renew short test permission after container startup
                await worker.cycle("paper")
                assert h.broker_app.state.contexts["paper"]["positions"]["AAPL"] == 35.0
                assert h.broker_app.state.contexts["live"]["positions"]["AAPL"] == 100.0
                assert (await h.action("stop_all"))[1].status_code == 200
                await h.sync_clean()
                count = len(h.broker_app.state.contexts["paper"]["requests"])
                await worker.cycle("paper")
                assert len(
                    h.broker_app.state.contexts["paper"]["requests"]) == count
                assert worker.strategies["paper"][0][0].container is None
            finally:
                await close_worker(worker)

    asyncio.run(scenario())


@requires_sandbox
def test_strategy_isolation_signals_and_timeout():
    source = """from hqg_algorithms import Strategy, TargetWeights, Hold, Liquidate
import os
import socket

class Isolated(Strategy):
    universe = ["AAPL"]
    def __init__(self):
        self.cycles = 0
        assert "PROVIDER_WORKER_SECRET" not in os.environ
        assert not os.path.exists("/var/run/docker.sock")
        try:
            with open("/app/escape", "w") as f:
                f.write("escape")
        except OSError:
            pass
        else:
            raise ValueError("Writable root")
        try:
            socket.create_connection(("1.1.1.1", 80), timeout=0.1)
        except OSError:
            pass
        else:
            raise ValueError("Network accessible")
    def on_data(self, data, portfolio):
        assert data.close("AAPL") == 100
        assert portfolio.equity == 10000
        self.cycles += 1
        return TargetWeights({"AAPL": 0.6}) if self.cycles == 1 else Hold() if self.cycles == 2 else Liquidate()
"""
    package = {
        "strategy_id": "isolated",
        "version": 1,
        "entrypoint": "main.py",
        "files": {"main.py": source},
    }
    portfolio = {"equity": 10000, "cash": 10000,
                 "positions": {}, "weights": {}}
    sandbox = Sandbox(package)
    try:
        assert sandbox.on_data({"AAPL": 100}, portfolio) == {"AAPL": 0.6}
        assert sandbox.on_data({"AAPL": 100}, portfolio) is None
        assert sandbox.on_data({"AAPL": 100}, portfolio) == {}
        sandbox.container.reload()
        config = sandbox.container.attrs
        assert config["HostConfig"]["NetworkMode"] == "none"
        assert config["Config"]["User"] == "65534:65534"
        assert config["Mounts"] == []
    finally:
        sandbox.close()
    package["files"]["main.py"] = source.replace(
        'assert data.close("AAPL") == 100', "while True: pass"
    )
    sandbox = Sandbox(package, timeout=1)
    with pytest.raises(TimeoutError):
        sandbox.on_data({"AAPL": 100}, portfolio)
    assert sandbox.container is None


@requires_sandbox
@pytest.mark.parametrize("initial_positions", [{}, {"AAPL": 40.0, "MSFT": 20.0}])
@pytest.mark.parametrize("include_balanced", [False, True])
def test_worker_hold_preserves_positions_and_liquidate_clears_targets(
    tmp_path, initial_positions, include_balanced
):
    async def scenario():
        async with Harness(tmp_path) as h:
            h.provider.max_ttl = h.dashboard_app.state.control.ttl = 15
            source = """from hqg_algorithms import Strategy, TargetWeights, Hold, Liquidate
class Signals(Strategy):
    universe = ["AAPL", "MSFT"]
    def __init__(self):
        self.cycles = 0
    def on_data(self, data, portfolio):
        self.cycles += 1
        if self.cycles in (1, 2, 4):
            return Hold()
        if self.cycles == 3:
            return TargetWeights({"AAPL": 0.6})
        return Liquidate()
"""
            strategy_id, version = await saved_strategy(h, source)
            assert (await register(h, strategy_id, version))[1].status_code == 200
            await h.sync_clean()
            identity = (await h.status())["approved_strategies"][-1]
            worker = new_worker(h)
            try:
                await worker.register()
                await h.sync_clean()
                strategies = [{**identity, "allocation": 0.5}]
                if include_balanced:
                    strategies.append({**APPROVED[0], "allocation": 0.5})
                await configure(h, strategies)
                context = h.broker_app.state.contexts["paper"]
                context["positions"] = dict(initial_positions)
                await h.resume("paper")
                await worker.cycle("paper")
                assert context["positions"] == initial_positions
                await worker.cycle("paper")
                assert context["positions"] == initial_positions
                await worker.cycle("paper")
                assert context["positions"]["AAPL"] == (
                    50.0 if include_balanced else 30.0
                )
                assert context["positions"].get("MSFT", 0) == (
                    10.0 if include_balanced else 0
                )
                count = len([r for r in context["requests"]
                            if r["method"] == "POST"])
                await worker.cycle("paper")
                assert (
                    len([r for r in context["requests"] if r["method"] == "POST"])
                    == count
                )
                await worker.cycle("paper")
                if include_balanced:
                    assert context["positions"] == {"AAPL": 20.0, "MSFT": 10.0}
                else:
                    assert all(
                        quantity == 0 for quantity in context["positions"].values()
                    )
            finally:
                await close_worker(worker)

    asyncio.run(scenario())


@requires_sandbox
def test_strategy_failure_latches_provider_pause(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            h.provider.max_ttl = h.dashboard_app.state.control.ttl = 15
            source = SOURCE.replace(
                "from helper import TARGET", "TARGET = 0.6"
            ).replace("self.cycles += 1", "raise ValueError('broken strategy')")
            strategy_id, version = await saved_strategy(h, source)
            assert (await register(h, strategy_id, version))[1].status_code == 200
            await h.sync_clean()
            identity = (await h.status())["approved_strategies"][-1]
            worker = new_worker(h)
            try:
                await worker.register()
                await h.sync_clean()
                await configure(h, [{**identity, "allocation": 1}])
                await h.resume("paper")
                with pytest.raises(ValueError, match="broken strategy"):
                    await worker.cycle("paper")
                assert h.provider.modes["paper"]["gate_open"] is False
                assert (
                    h.provider.modes["paper"]["stop_reason"]
                    == "strategy_execution_failed"
                )
                assert "broken strategy" in h.provider.modes["paper"]["strategy_error"]
                await h.sync_clean()
                assert h.provider.modes["paper"]["gate_open"] is False
                assert not h.broker_app.state.contexts["paper"]["positions"]
            finally:
                await close_worker(worker)

    asyncio.run(scenario())
