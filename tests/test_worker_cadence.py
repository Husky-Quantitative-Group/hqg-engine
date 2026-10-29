import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from hqg_algorithms import BarSize, Cadence

from src.control.worker import Worker
from src.control.packages import validate_package
from src.control.sandbox import Sandbox


def bar(timestamp, close=100):
    return {"timestamp": timestamp, "open": 98, "high": 103, "low": 96,
            "close": close, "volume": 12345}


class Broker:
    def __init__(self):
        self.data = {}
        self.bar_requests = []

    async def bars(self, symbols, size, execution):
        self.bar_requests.append((tuple(symbols), size, execution))
        return self.data.get(size, {})

    async def request(self, method, path):
        return [] if path.endswith("positions") else {"equity": "10000", "cash": "10000"}

    async def quotes(self, symbols):
        return {symbol: 100 for symbol in symbols}


def strategy(size=BarSize.DAILY, signals=None, universe=None):
    return SimpleNamespace(
        universe=universe or ["AAPL"], cadence=Cadence(bar_size=size),
        on_data=Mock(side_effect=signals) if signals is not None else Mock(return_value={"AAPL": 0.4}),
    )


@asynccontextmanager
async def worker_context(strategies, warmup=None):
    broker = Broker()
    worker = Worker("http://127.0.0.1:8013", "test", brokers={"paper": broker, "live": broker})
    await worker.client.aclose()
    worker.revision, worker.strategies = 1, strategies
    requests, responses = [], []
    state = {
        "configuration_revision": 1, "configuration": None,
        "account_id": "account", "boot_id": "boot", "worker_session": worker.session_id,
        "control_version": 1, "configuration_digest": "digest",
        "modes": {mode: {"gate_open": True} for mode in ("paper", "live")},
    }

    def respond(request):
        if request.url.path.endswith("/state"):
            return httpx.Response(200, json=state)
        if request.url.path.endswith("/failure"):
            return httpx.Response(200, json={})
        if request.url.path.endswith("/warmup"):
            return httpx.Response(200, json={"weights": warmup(json.loads(request.content))})
        requests.append(json.loads(request.content))
        return httpx.Response(responses.pop(0) if responses else 200, json={"status": "completed"})

    worker.client = httpx.AsyncClient(
        base_url="http://127.0.0.1:8013/", transport=httpx.MockTransport(respond)
    )
    try:
        yield worker, broker, state, requests, responses
    finally:
        await worker.client.aclose()


@pytest.mark.parametrize("size,first,second", [
    (BarSize.DAILY, "2026-10-01", "2026-10-02"),
    (BarSize.WEEKLY, "2026-09-21", "2026-09-28"),
    (BarSize.MONTHLY, "2026-08-01", "2026-09-01"),
    (BarSize.QUARTERLY, "2026-04-01", "2026-07-01"),
])
def test_worker_advances_each_cadence_once_per_new_completed_bar(size, first, second):
    async def scenario():
        instance = strategy(size)
        async with worker_context({"paper": [(instance, 1)]}) as (worker, broker, _, requests, _):
            broker.data[size.value] = {"AAPL": bar(first + "T00:00:00-04:00")}
            await worker.cycle("paper")
            # A correction to the same bar must not advance a stateful strategy.
            broker.data[size.value]["AAPL"]["close"] = 101
            await worker.cycle("paper")
            assert instance.on_data.call_count == len(requests) == 1
            broker.data[size.value] = {"AAPL": bar(second + "T00:00:00-04:00")}
            await worker.cycle("paper")
            broker.data[size.value] = {"AAPL": bar(first + "T00:00:00-04:00")}
            await worker.cycle("paper")
            assert instance.on_data.call_count == len(requests) == 2
            assert "timestamp" not in instance.on_data.call_args.args[0]["AAPL"]
            assert all(request[1:] == (size.value, "close_to_close") for request in broker.bar_requests)
    asyncio.run(scenario())


def test_mixed_cadences_reuse_other_strategies_targets_and_keep_modes_independent():
    async def scenario():
        daily = strategy(signals=[{"AAPL": 0.4}, {"AAPL": 0.2}, None])
        weekly = strategy(BarSize.WEEKLY)
        live = strategy()
        async with worker_context({"paper": [(daily, 0.5), (weekly, 0.5)], "live": [(live, 1)]}) as (
            worker, broker, _, requests, _,
        ):
            broker.data = {"1d": {"AAPL": bar("2026-10-01T00:00:00-04:00")},
                           "1w": {"AAPL": bar("2026-09-21T00:00:00-04:00")}}
            await worker.cycle("paper")
            await worker.cycle("live")
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            await worker.cycle("paper")
            assert requests[-1]["target_weights"]["AAPL"] == pytest.approx(0.3)
            assert weekly.on_data.call_count == live.on_data.call_count == 1
            broker.data["1d"] = {"AAPL": bar("2026-10-05T00:00:00-04:00")}
            await worker.cycle("paper")  # Hold advances the bar without trading.
            await worker.cycle("paper")
            assert daily.on_data.call_count == len(requests) == 3
    asyncio.run(scenario())


def test_incomplete_or_misaligned_universe_waits_for_all_symbols():
    async def scenario():
        instance = strategy(universe=["AAPL", "MSFT"])
        async with worker_context({"paper": [(instance, 1)]}) as (worker, broker, _, requests, _):
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            await worker.cycle("paper")
            broker.data["1d"]["MSFT"] = bar("2026-10-01T00:00:00-04:00")
            await worker.cycle("paper")
            assert not requests and not instance.on_data.called
            broker.data["1d"]["MSFT"] = bar("2026-10-02T00:00:00-04:00")
            await worker.cycle("paper")
            assert instance.on_data.call_count == len(requests) == 1
    asyncio.run(scenario())


def test_temporary_rebalance_rejection_retries_signal_without_replaying_bar():
    async def scenario():
        instance = strategy()
        async with worker_context({"paper": [(instance, 1)]}) as (worker, broker, _, requests, responses):
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            responses.append(409)
            with pytest.raises(httpx.HTTPStatusError):
                await worker.cycle("paper")
            await worker.cycle("paper")
            await worker.cycle("paper")
            assert requests[0] == requests[1]
            assert instance.on_data.call_count == 1
            assert not worker.pending_rebalances
    asyncio.run(scenario())


def test_pause_discards_pending_targets_and_resets_strategy_bar_state():
    async def scenario():
        instance = strategy()
        async with worker_context({"paper": [(instance, 1)]}) as (worker, broker, state, requests, responses):
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            responses.append(409)
            with pytest.raises(httpx.HTTPStatusError):
                await worker.cycle("paper")
            state["modes"]["paper"]["gate_open"] = False
            await worker.cycle("paper")
            assert len(requests) == 1
            assert not worker.last_weights and not worker.last_bars and not worker.pending_rebalances
    asyncio.run(scenario())


def test_new_configuration_discards_pending_targets_and_restarts_bar_tracking(monkeypatch):
    package = {
        "strategy_id": "replacement", "version": 1, "entrypoint": "main.py",
        "files": {"main.py": """from hqg_algorithms import Strategy
class Replacement(Strategy):
    universe = ["AAPL"]
    def on_data(self, data, portfolio): pass
"""},
    }
    identity, _ = validate_package(package)
    replacement = Mock(return_value={"AAPL": 0.2})
    monkeypatch.setattr(Sandbox, "on_data", replacement)

    async def scenario():
        original = strategy()
        async with worker_context({"paper": [(original, 1)]}) as (worker, broker, state, requests, responses):
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            responses.append(409)
            with pytest.raises(httpx.HTTPStatusError):
                await worker.cycle("paper")
            state.update(
                configuration_revision=2, configuration_digest="replacement",
                strategy_packages={identity["source_digest"]: package},
                configuration={"modes": {"paper": {"strategies": [{**identity, "allocation": 1}]}}},
            )
            await worker.cycle("paper")
            assert original.on_data.call_count == replacement.call_count == 1
            assert requests[-1]["target_weights"] == {"AAPL": 0.2}
            assert requests[-1]["context"]["configuration_digest"] == "replacement"
            assert not worker.pending_rebalances
    asyncio.run(scenario())


def packaged(signals):
    instance = strategy(signals=signals)
    instance.identity = {"strategy_id": "probe", "version": 1, "source_digest": "sha256:probe"}
    return instance


def test_cold_hold_is_seeded_from_warmup_once_and_rebalanced():
    async def scenario():
        instance = packaged([None, None])
        warmup = Mock(return_value={"AAPL": 0.5})
        async with worker_context({"paper": [(instance, 1)]}, warmup) as (worker, broker, _, requests, _):
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            await worker.cycle("paper")
            assert warmup.call_args.args[0] == {
                "source_digest": "sha256:probe", "timestamp": "2026-10-02T00:00:00-04:00",
            }
            assert [r["target_weights"] for r in requests] == [{"AAPL": 0.5}]
            broker.data["1d"] = {"AAPL": bar("2026-10-05T00:00:00-04:00")}
            await worker.cycle("paper")  # Hold carries the seed without warming up again.
            assert warmup.call_count == len(requests) == 1
            assert worker.last_weights[("paper", 0)] == {"AAPL": 0.5}
    asyncio.run(scenario())


def test_missing_warmup_target_keeps_the_mode_cold():
    async def scenario():
        instance = packaged([None])
        warmup = Mock(return_value=None)
        async with worker_context({"paper": [(instance, 1)]}, warmup) as (worker, broker, _, requests, _):
            broker.data["1d"] = {"AAPL": bar("2026-10-02T00:00:00-04:00")}
            assert await worker.cycle("paper") is None
            assert warmup.call_count == 1
            assert not requests and not worker.last_weights
    asyncio.run(scenario())
