import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import AsyncMock

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
async def worker_context(strategies):
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


def alternative_records(value=1):
    return [{'date': '2024-01-01T00:00:00', 'available_at': '2024-02-01T00:00:00+00:00', 'value': value}]


def test_alternative_data_is_fetched_once_for_shared_series_only_on_new_bars():
    async def scenario():
        first, second = strategy(), strategy()
        first.alt_data = second.alt_data = ['FRED.GDP']
        async with worker_context({'paper': [(first, 0.5), (second, 0.5)]}) as (worker, broker, _, requests, _):
            worker.alternative_history = AsyncMock(return_value={'FRED.GDP': alternative_records(3)})
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            await worker.cycle('paper')
            await worker.cycle('paper')
            assert worker.alternative_history.await_count == 1
            assert worker.alternative_history.call_args.args[1] == ['FRED.GDP']
            assert len(requests) == 1
            assert first.on_data.call_args.args[2] == {'FRED.GDP': {'2024-01-01T00:00:00': 3}}
    asyncio.run(scenario())


def test_strategies_with_different_lookbacks_share_untrimmed_series():
    async def scenario():
        first, second = strategy(), strategy()
        first.alt_data, first.lookback = ['FRED.GDP', 'FRED.CPI'], 2
        second.alt_data, second.lookback = ['FRED.GDP'], 3
        async with worker_context({'paper': [(first, 0.5), (second, 0.5)]}) as (worker, broker, _, _, _):
            records = {
                'FRED.GDP': alternative_records(3),
                'FRED.CPI': [{**alternative_records(2)[0], 'date': '2024-01-10T00:00:00'}],
            }
            worker.alternative_history = AsyncMock(return_value=records)
            worker.replay = AsyncMock()
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            await worker.cycle('paper')
            worker.alternative_history.assert_awaited_once()
            assert worker.alternative_history.call_args.args[1] == ['FRED.CPI', 'FRED.GDP']
            assert first.on_data.call_args.args[2] == {
                'FRED.GDP': {'2024-01-01T00:00:00': 3},
                'FRED.CPI': {'2024-01-10T00:00:00': 2},
            }
            assert second.on_data.call_args.args[2] == {'FRED.GDP': {'2024-01-01T00:00:00': 3}}
            assert worker.replay.call_args_list[0].args[4]['FRED.GDP'][0]['value'] == 3
            assert worker.replay.call_args_list[1].args[4]['FRED.GDP'][0]['value'] == 3
    asyncio.run(scenario())


@pytest.mark.parametrize('count', ['20', 0, -1, 1001, True, 2.5])
@pytest.mark.parametrize('has_previous_target', [False, True])
def test_invalid_alt_lookback_defers_only_affected_strategy(caplog, count, has_previous_target):
    async def scenario():
        invalid, valid = strategy(), strategy()
        invalid.alt_data, invalid.lookback = ['FRED.GDP', 'FRED.CPI'], count
        valid.alt_data = ['FRED.GDP']
        async with worker_context({'paper': [(invalid, 0.5), (valid, 0.5)]}) as (worker, broker, state, requests, _):
            worker.alternative_history = AsyncMock(return_value={'FRED.GDP': alternative_records()})
            if has_previous_target:
                worker.last_weights[('paper', 0)] = {'AAPL': 0.2}
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            await worker.cycle('paper')
            assert not invalid.on_data.called
            assert valid.on_data.call_count == 1
            assert worker.alternative_history.call_args.args[1] == ['FRED.GDP']
            assert ('paper', 0) not in worker.pending_bars
            assert ('paper', 0) not in worker.last_bars
            assert state['modes']['paper']['gate_open']
            assert 'Deferring paper strategy 0: lookback' in caplog.text
            if has_previous_target:
                assert worker.last_weights[('paper', 0)] == {'AAPL': 0.2}
                assert requests[0]['target_weights']['AAPL'] == pytest.approx(0.3)
            else:
                assert ('paper', 0) not in worker.last_weights
                assert not requests  # Preserve main's initial-target rebalance gate.
    asyncio.run(scenario())


def test_alt_failure_keeps_oldest_bar_and_previous_targets_without_closing_gate():
    async def scenario():
        instance = strategy()
        instance.alt_data = ['FRED.GDP']
        async with worker_context({'paper': [(instance, 1)]}) as (worker, broker, state, requests, _):
            worker.alternative_history = AsyncMock(return_value={'FRED.GDP': alternative_records()})
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00', 100)}
            await worker.cycle('paper')
            worker.alternative_history.return_value = {}
            broker.data['1d'] = {'AAPL': bar('2026-10-02T00:00:00-04:00', 101)}
            await worker.cycle('paper')
            broker.data['1d'] = {'AAPL': bar('2026-10-05T00:00:00-04:00', 102)}
            await worker.cycle('paper')
            assert worker.last_weights[('paper', 0)] == {'AAPL': 0.4}
            assert len(requests) == 1 and instance.on_data.call_count == 1
            assert state['modes']['paper']['gate_open']
            worker.alternative_history.return_value = {'FRED.GDP': alternative_records()}
            await worker.cycle('paper')
            await worker.cycle('paper')
            assert [call.args[0]['AAPL']['close'] for call in instance.on_data.call_args_list] == [100, 101, 102]
            assert not worker.pending_bars[('paper', 0)]
    asyncio.run(scenario())


def test_initial_alt_failure_preserves_upstream_initial_target_gate():
    async def scenario():
        missing, ready = strategy(), strategy()
        missing.alt_data = ['FRED.GDP']
        async with worker_context({'paper': [(missing, 0.5), (ready, 0.5)]}) as (worker, broker, state, requests, _):
            worker.alternative_history = AsyncMock(return_value={})
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            await worker.cycle('paper')
            assert not missing.on_data.called and ready.on_data.call_count == 1
            assert not requests and state['modes']['paper']['gate_open']
            worker.alternative_history.return_value = {'FRED.GDP': alternative_records()}
            await worker.cycle('paper')
            assert len(requests) == 1 and missing.on_data.call_count == 1
    asyncio.run(scenario())


def test_pausing_discards_pending_alt_bars_and_fetches_nothing():
    async def scenario():
        instance = strategy()
        instance.alt_data = ['FRED.GDP']
        async with worker_context({'paper': [(instance, 1)]}) as (worker, broker, state, _, _):
            worker.alternative_history = AsyncMock(return_value={})
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            await worker.cycle('paper')
            assert worker.pending_bars[('paper', 0)]
            state['modes']['paper']['gate_open'] = False
            await worker.cycle('paper')
            assert not worker.pending_bars
            assert worker.alternative_history.await_count == 1
    asyncio.run(scenario())


def test_alt_lookback_filters_future_revisions_but_live_uses_latest():
    async def scenario():
        instance = strategy(signals=[{'AAPL': 0.1}, {'AAPL': 0.2}, {'AAPL': 0.4}])
        instance.alt_data, instance.lookback = ['FRED.GDP'], 2
        async with worker_context({'paper': [(instance, 1)]}) as (worker, broker, _, requests, _):
            records = [
                {'date': '2023-01-01T00:00:00', 'available_at': release, 'value': value}
                for release, value in [('2023-12-31T00:00:00+00:00', 1),
                                       ('2024-02-01T00:00:00+00:00', 2),
                                       ('2025-01-01T00:00:00+00:00', 99)]
            ]
            worker.alternative_history = AsyncMock(return_value={'FRED.GDP': records})
            broker.history = AsyncMock(return_value=[
                {'timestamp': '2024-01-15T00:00:00+00:00', 'bars': {'AAPL': {'close': 100}}},
                {'timestamp': '2024-02-15T00:00:00+00:00', 'bars': {'AAPL': {'close': 101}}},
            ])
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            await worker.cycle('paper')
            assert [call.args[2]['FRED.GDP']['2023-01-01T00:00:00']
                    for call in instance.on_data.call_args_list] == [1, 2, 99]
            assert requests[0]['target_weights'] == {'AAPL': 0.4}
            await worker.cycle('paper')
            broker.history.assert_awaited_once()
    asyncio.run(scenario())


def test_alternative_http_failures_are_deferred_not_reported_as_strategy_errors():
    async def scenario():
        instance = strategy()
        instance.alt_data = ['FRED.GDP']
        async with worker_context({'paper': [(instance, 1)]}) as (worker, broker, _, _, _):
            paths = []

            def respond(request):
                paths.append(request.url.path)
                if request.url.path.endswith('/state'):
                    return httpx.Response(200, json=state)
                return httpx.Response(503)

            state = {'worker_session': worker.session_id, 'configuration_revision': 1,
                     'account_id': 'account', 'boot_id': 'boot', 'control_version': 1,
                     'configuration_digest': 'digest', 'modes': {'paper': {'gate_open': True}}}
            await worker.client.aclose()
            worker.client = httpx.AsyncClient(base_url='http://127.0.0.1:8013/', transport=httpx.MockTransport(respond))
            broker.data['1d'] = {'AAPL': bar('2026-10-01T00:00:00-04:00')}
            assert await worker.cycle('paper') is None
            assert '/internal/alternative-data' in paths
            assert '/internal/failure' not in paths
            assert not instance.on_data.called
    asyncio.run(scenario())
