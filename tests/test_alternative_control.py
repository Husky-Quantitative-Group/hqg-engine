import asyncio
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest
from fastapi import HTTPException

from src.alt_data import AlternativeData, select_history
from src.control import alternative
from src.control.alternative import AlternativeService, lookback_start
from src.control.packages import validate_package
from src.control.sandbox import Sandbox
from src.provider_instance.server import create_app


SOURCE = '''from hqg_algorithms import Strategy, TargetWeights
class Macro(Strategy):
    universe = ["SPY"]
    alt_data = ["FRED.GDP"]
    def on_data(self, data, portfolio):
        assert data.alt_value("FRED.GDP") == 2
        history = data.alt_series("FRED.GDP")
        assert list(history) == sorted(history)
        try:
            history[next(iter(history))] = 999
        except TypeError:
            pass
        else:
            raise ValueError("History must be immutable")
        return TargetWeights({"SPY": 0.5})
'''
PACKAGE = {'strategy_id': 'macro', 'version': 1, 'entrypoint': 'main.py', 'files': {'main.py': SOURCE}}
CONTEXT = {'account_id': 'account', 'mode': 'paper', 'boot_id': 'boot',
           'worker_session': 'worker', 'control_version': 1, 'configuration_digest': 'digest'}


class Feed:
    providers = {'FRED': object()}

    def __init__(self):
        self.calls = []
        self.starts = []

    def get_alt_data(self, ids, **kwargs):
        self.calls.append(ids)
        self.starts.append(kwargs['start'])
        return {s: pd.DataFrame({
            'value': [1, 2], 'available_at': ['2024-02-01', '2024-03-01'],
        }, index=pd.to_datetime(['2024-01-01', '2024-01-01'])) for s in ids}


def configured_state():
    identity, _ = validate_package(PACKAGE)
    return {'configuration': {'modes': {'paper': {'strategies': [identity]}}},
            'strategy_packages': {identity['source_digest']: PACKAGE}}


@pytest.fixture
def clock(monkeypatch):
    now = [datetime(2024, 1, 17, 17, tzinfo=timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0].astimezone(tz)

    monkeypatch.setattr(alternative, 'datetime', Clock)
    return now


@pytest.mark.parametrize('count,today,expected', [
    (1, '2026-11-02', '2026-10-29'),  # Monday skips the weekend and subtracts 2 weekdays.
    (1, '2026-11-01', '2026-10-29'),  # Sunday also counts backward from Friday.
    (1, '2026-11-28', '2026-11-26'),  # Holidays are not explicitly excluded.
    (20, '2026-11-02', '2026-10-01'),  # 20 becomes 22 weekdays.
    (21, '2026-11-02', '2026-09-29'),  # Fractional buffer rounds up to 24 weekdays.
    (1000, '2026-11-02', '2022-08-15'),
])
def test_lookback_start_subtracts_buffered_weekdays(count, today, expected):
    assert lookback_start(count, date.fromisoformat(today)) == date.fromisoformat(expected)


def test_provider_only_serves_declared_series_and_preserves_vintages(clock):
    async def scenario():
        feed = Feed()
        service = AlternativeService()
        service.cache = AlternativeData(['FRED.GDP'], feed=feed, history_start='2020-01-01')
        calls = []

        def admit(context):
            assert context == CONTEXT
            calls.append(context)

        provider = SimpleNamespace(state=configured_state, admit=admit, shutdown=AsyncMock())
        app = create_app(provider, 'test-secret', background=False, alternative=service)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://localhost') as client:
                body = {'context': CONTEXT, 'series': ['FRED.GDP']}
                assert (await client.post('/internal/alternative-data', json=body)).status_code == 403
                headers = {'Authorization': 'Bearer test-secret'}
                result = await client.post('/internal/alternative-data', json=body, headers=headers)
                assert result.status_code == 200
                payload = result.json()
                assert payload['ready'] == ['FRED.GDP']
                assert len(payload['history']['FRED.GDP']) == 2
                assert feed.starts == [datetime(2024, 1, 15)]
                assert len(calls) == 2  # Check control context before and after the fetch.
                assert select_history(payload['history']['FRED.GDP']) == {'2024-01-01T00:00:00': 2}
                assert (await client.post('/internal/alternative-data',
                        json={'context': CONTEXT, 'series': ['FRED.UNDECLARED']}, headers=headers)).status_code == 400
                assert feed.calls == [['FRED.GDP']]
    asyncio.run(scenario())


def test_provider_rechecks_control_context_after_fetch():
    async def scenario():
        current = [True]

        def admit(context):
            if not current[0]:
                raise HTTPException(409, 'Context changed')

        async def fetch(*args):
            current[0] = False
            return {'ready': ['FRED.GDP'], 'history': {}}

        provider = SimpleNamespace(state=configured_state, admit=admit, shutdown=AsyncMock())
        app = create_app(provider, 'test-secret', background=False,
                         alternative=SimpleNamespace(fetch=fetch))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://localhost') as client:
                response = await client.post('/internal/alternative-data',
                    json={'context': CONTEXT, 'series': ['FRED.GDP']},
                    headers={'Authorization': 'Bearer test-secret'})
                assert response.status_code == 409
    asyncio.run(scenario())


def test_shared_series_use_earliest_start_without_trimming_and_roll_forward(clock):
    async def scenario():
        state = configured_state()
        for name, count, series in [('long', 10, ['FRED.GDP']),
                                    ('other', 3, ['FRED.GDP', 'FRED.CPI'])]:
            source = SOURCE.replace('    alt_data = ["FRED.GDP"]',
                                    f'    lookback = {count}\n    alt_data = {series!r}')
            package = {**PACKAGE, 'strategy_id': name, 'files': {'main.py': source}}
            identity, _ = validate_package(package)
            state['configuration']['modes']['paper']['strategies'].append(identity)
            state['strategy_packages'][identity['source_digest']] = package
        service = AlternativeService()
        service.cache = SimpleNamespace(refresh=AsyncMock(return_value={'FRED.GDP', 'FRED.CPI'}),
            records=lambda ids: {s: [
                {'date': '2024-01-01T00:00:00', 'available_at': '2024-02-01T00:00:00+00:00', 'value': 1},
                {'date': '2024-01-10T00:00:00', 'available_at': '2024-02-01T00:00:00+00:00', 'value': 2},
            ] for s in ids})
        result = await service.fetch(state, 'paper', ['FRED.GDP', 'FRED.CPI'])
        service.cache.refresh.assert_awaited_with(['FRED.GDP', 'FRED.CPI'],
            starts={'FRED.GDP': '2024-01-02', 'FRED.CPI': '2024-01-11'})
        assert len(result['history']['FRED.GDP']) == 2
        # Pass returned observations through, even if a feed supplies extra history.
        assert len(result['history']['FRED.CPI']) == 2
        clock[0] = datetime(2024, 1, 18, 17, tzinfo=timezone.utc)
        await service.fetch(state, 'paper', ['FRED.GDP', 'FRED.CPI'])
        service.cache.refresh.assert_awaited_with(['FRED.GDP', 'FRED.CPI'],
            starts={'FRED.GDP': '2024-01-03', 'FRED.CPI': '2024-01-12'})
    asyncio.run(scenario())


@pytest.mark.parametrize('count', ['20', 0, -1, 1001, True, 2.5])
def test_invalid_lookback_does_not_block_valid_shared_series(clock, caplog, count):
    async def scenario():
        state = configured_state()
        source = SOURCE.replace('    alt_data = ["FRED.GDP"]',
            f'    lookback = {count!r}\n    alt_data = ["FRED.GDP", "FRED.CPI"]')
        package = {**PACKAGE, 'strategy_id': 'invalid', 'files': {'main.py': source}}
        identity, _ = validate_package(package)
        state['configuration']['modes']['paper']['strategies'].append(identity)
        state['strategy_packages'][identity['source_digest']] = package
        feed = Feed()
        service = AlternativeService()
        service.cache = AlternativeData(['FRED.GDP'], feed=feed, history_start='2020-01-01')

        result = await service.fetch(state, 'paper', ['FRED.GDP', 'FRED.CPI'])
        assert result['ready'] == ['FRED.GDP']
        assert set(result['history']) == {'FRED.GDP'}
        assert feed.calls == [['FRED.GDP']]
        assert 'invalid' in caplog.text and 'lookback' in caplog.text

        # Only invalid strategies remain: return unavailable without constructing a feed.
        state['configuration']['modes']['paper']['strategies'] = [identity]
        service = AlternativeService()
        assert await service.fetch(state, 'paper', ['FRED.GDP', 'FRED.CPI']) == {
            'ready': [], 'history': {},
        }
        assert service.cache is None
        with pytest.raises(ValueError, match='declared'):
            await service.fetch(state, 'paper', ['FRED.UNDECLARED'])
    asyncio.run(scenario())


def test_sandbox_extracts_metadata_and_transports_history(monkeypatch):
    sandbox = Sandbox(PACKAGE)
    assert sandbox.alt_data == ['FRED.GDP']
    sandbox.container = object()  # Exercise the protocol without launching Docker.
    seen = []
    monkeypatch.setattr(sandbox, 'exchange', lambda message: seen.append(message) or {'weights': {'SPY': 0.5}})
    history = {'FRED.GDP': {'2024-01-01T00:00:00': 2}}
    assert sandbox.on_data({}, {}, history) == {'SPY': 0.5}
    assert seen[0]['alt_history'] == history


def test_sandbox_runner_builds_strategy_slice_from_wire_history():
    portfolio = {'equity': 100, 'cash': 100, 'positions': {}, 'weights': {}}
    message = {'bars': {'SPY': {'open': 100, 'high': 100, 'low': 100, 'close': 100, 'volume': 1}},
               'portfolio': portfolio,
               'alt_history': {'FRED.GDP': {'2024-01-01T00:00:00': 2}}}
    messages = [{'package': PACKAGE}, message, {**message, 'alt_history': {'FRED.OTHER': {}}}]
    result = subprocess.run([sys.executable, '-u', '-m', 'src.control.sandbox_runner'],
        input=''.join(json.dumps(item) + '\n' for item in messages),
        text=True, capture_output=True, timeout=15, check=True,
        cwd=Path(__file__).resolve().parents[1], env={'PATH': os.environ.get('PATH', '')})
    output = [json.loads(line) for line in result.stdout.splitlines()]
    assert output[0] == {'ready': True}
    assert output[1] == {'weights': {'SPY': 0.5}}
    assert 'Alternative history differs' in output[2]['error']
