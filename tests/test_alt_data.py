import asyncio
from datetime import datetime, timezone
from threading import Event

import pandas as pd
import pytest

from src.alt_data import AlternativeData, _ProviderFetch, normalize_series


def frame(value=1.0):
    return pd.DataFrame({
        'value': [value], 'available_at': [datetime(2024, 2, 1)],
    }, index=pd.DatetimeIndex([datetime(2024, 1, 1)], name='date'))


class Feed:
    providers = {'FRED': object(), 'KALSHI': object()}

    def __init__(self):
        self.calls = []
        self.fail = set()
        self.value = 1.0

    def get_alt_data(self, ids, *, start, end):
        self.calls.append((ids, start, end))
        if set(ids) & self.fail:
            raise RuntimeError('provider unavailable')
        return {s: frame(self.value) for s in ids}


def test_normalization_and_configuration(monkeypatch):
    assert normalize_series([' fred.gdp ', 'FRED.GDP', 'KALSHI.MARKET']) == [
        'FRED.GDP', 'KALSHI.MARKET',
    ]
    for invalid in [['GDP'], [''], [None], 'FRED.GDP']:
        with pytest.raises(ValueError):
            normalize_series(invalid)
    monkeypatch.delenv('ALT_DATA_HISTORY_START', raising=False)
    for start in [None, 'bad', '9999-01-01']:
        with pytest.raises(ValueError, match='ALT_DATA_HISTORY_START'):
            AlternativeData(['FRED.GDP'], feed=Feed(), history_start=start)
    with pytest.raises(ValueError, match='Unsupported'):
        AlternativeData(['UNKNOWN.GDP'], feed=Feed(), history_start='2020-01-01')


def test_provider_failure_cooldown_and_recovery():
    async def scenario():
        now = [100.0]
        feed = Feed()
        cache = AlternativeData(['FRED.GDP', 'KALSHI.MARKET'], feed=feed,
                                history_start='2020-01-01', clock=lambda: now[0])
        await cache.refresh(cache.ids)
        old = cache.snapshot(['FRED.GDP'])
        feed.fail = {'FRED.GDP'}
        feed.value = 2
        assert await cache.refresh(cache.ids) == {'KALSHI.MARKET'}
        assert cache.snapshot(['FRED.GDP']) == old
        count = len(feed.calls)
        assert await cache.refresh(['FRED.GDP']) == set()
        assert len(feed.calls) == count
        now[0] += 60
        feed.fail.clear()
        assert await cache.refresh(['FRED.GDP']) == {'FRED.GDP'}
        assert old['FRED.GDP'][datetime(2024, 1, 1)] == 1
        assert cache.snapshot(['FRED.GDP'])['FRED.GDP'][datetime(2024, 1, 1)] == 2
        assert cache.received_at['FRED.GDP'].tzinfo == timezone.utc
        assert all(call[1] == datetime(2020, 1, 1) for call in feed.calls)
        with pytest.raises(TypeError):
            old['FRED.GDP'][datetime(2024, 1, 1)] = 7
    asyncio.run(scenario())


def test_latest_revision_and_availability_are_separate():
    source = pd.DataFrame({
        'value': [3, 1, 2],
        'available_at': ['2024-03-01', '2024-02-01', '2024-02-15'],
    }, index=pd.DatetimeIndex(['2024-01-01'] * 3))
    history, releases = AlternativeData._prepare(source)
    assert history == {datetime(2024, 1, 1): 3}
    assert releases[datetime(2024, 1, 1)] == datetime(2024, 3, 1, tzinfo=timezone.utc)
    # Live arrivals are usable even when provider availability is an EOD estimate.
    future = frame()
    future['available_at'] = [datetime(2099, 1, 1)]
    assert AlternativeData._prepare(future)[0]


def test_history_is_sorted_by_observation_date_after_selecting_revisions():
    source = pd.DataFrame({
        'value': [20, 10, 30, 11],
        'available_at': ['2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01'],
    }, index=pd.DatetimeIndex(['2024-02-01', '2024-01-01', '2024-03-01', '2024-01-01']))
    history, releases = AlternativeData._prepare(source)
    expected = [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)]
    assert list(history) == expected
    assert list(releases) == expected
    assert list(history.values()) == [11, 20, 30]
    assert releases[datetime(2024, 1, 1)] == datetime(2024, 5, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize('bad', [float('nan'), float('inf')])
def test_invalid_data_is_rejected(bad):
    with pytest.raises(ValueError):
        AlternativeData._prepare(frame(bad))


def test_fetch_runs_off_event_loop_and_serializes_requests():
    async def scenario():
        entered, release = Event(), Event()
        feed = Feed()
        original = feed.get_alt_data

        def slow(*args, **kwargs):
            entered.set()
            assert release.wait(5)
            return original(*args, **kwargs)

        feed.get_alt_data = slow
        cache = AlternativeData(['FRED.GDP'], feed=feed, history_start='2020-01-01')
        task = asyncio.create_task(cache.refresh(cache.ids))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            # This coroutine runs while the synchronous network call is blocked.
            second = asyncio.create_task(cache.refresh(cache.ids))
            await asyncio.sleep(0)
            assert not second.done()
        finally:
            release.set()
        assert await task == {'FRED.GDP'}
        assert await second == {'FRED.GDP'}
        assert len(feed.calls) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize('value', ['bad', '0', '-1', 'nan', 'inf'])
def test_invalid_fetch_timeout(value):
    with pytest.raises(ValueError, match='fetch_timeout'):
        AlternativeData(['FRED.GDP'], feed=Feed(), history_start='2020-01-01', fetch_timeout=value)


def test_series_failures_do_not_discard_same_provider_successes():
    async def scenario():
        feed = Feed()
        cache = AlternativeData(['FRED.GDP', 'FRED.CPI'], feed=feed, history_start='2020-01-01')
        await cache.refresh(cache.ids)
        old = cache.snapshot(['FRED.GDP'])
        feed.value = 2
        feed.fail = {'FRED.GDP'}
        assert await cache.refresh(cache.ids) == {'FRED.CPI'}
        assert cache.snapshot(['FRED.GDP']) == old
        assert cache.snapshot(['FRED.CPI'])['FRED.CPI'][datetime(2024, 1, 1)] == 2
    asyncio.run(scenario())


def test_engine_defensive_validation_isolated_per_series():
    async def scenario():
        feed = Feed()
        original = feed.get_alt_data

        def invalid(ids, **kwargs):
            if ids == ['FRED.GDP']:
                return {'FRED.GDP': frame(float('inf'))}
            return original(ids, **kwargs)

        feed.get_alt_data = invalid
        cache = AlternativeData(['FRED.GDP', 'FRED.CPI'], feed=feed, history_start='2020-01-01')
        assert await cache.refresh(cache.ids) == {'FRED.CPI'}
        assert 'FRED.GDP' not in cache.histories
    asyncio.run(scenario())


def test_provider_deadline_partial_success_and_no_overlapping_retries():
    async def scenario():
        now = [100.0]
        feed = Feed()
        cache = AlternativeData(['FRED.GDP', 'KALSHI.MARKET'], feed=feed,
                                history_start='2020-01-01', fetch_timeout=0.1,
                                clock=lambda: now[0])
        await cache.refresh(cache.ids)
        old = cache.snapshot(['FRED.GDP'])
        entered, release, fast_done = Event(), Event(), Event()
        calls = []

        def blocked(ids, **kwargs):
            calls.append(ids)
            if ids == ['FRED.GDP']:
                entered.set()
                assert release.wait(5)
                return {'FRED.GDP': frame(99)}
            fast_done.set()
            return {s: frame(2) for s in ids}

        feed.get_alt_data = blocked
        try:
            # Must complete without releasing the FRED thread. Kalshi runs concurrently.
            assert await asyncio.wait_for(cache.refresh(cache.ids), 2) == {'KALSHI.MARKET'}
            assert entered.is_set() and fast_done.is_set()
            assert cache.snapshot(['FRED.GDP']) == old
            assert cache.snapshot(['KALSHI.MARKET'])['KALSHI.MARKET'][datetime(2024, 1, 1)] == 2
            assert await cache.refresh(['FRED.GDP']) == set()  # cooldown
            now[0] += 60
            for _ in range(3):
                assert await cache.refresh(['FRED.GDP']) == set()  # still running
            assert calls.count(['FRED.GDP']) == 1
            worker = cache._inflight['FRED'].task
        finally:
            release.set()
        await asyncio.wait_for(asyncio.shield(worker), 2)
        # A late completion must not overwrite valid cached data.
        assert cache.snapshot(['FRED.GDP']) == old
        feed.get_alt_data = lambda ids, **kwargs: {s: frame(3) for s in ids}
        assert await cache.refresh(['FRED.GDP']) == {'FRED.GDP'}
        assert cache.snapshot(['FRED.GDP'])['FRED.GDP'][datetime(2024, 1, 1)] == 3
    asyncio.run(scenario())


def test_cancelling_waiter_keeps_provider_worker_tracked():
    async def scenario():
        feed = Feed()
        entered, release = Event(), Event()
        original = feed.get_alt_data

        def blocked(*args, **kwargs):
            entered.set()
            assert release.wait(5)
            return original(*args, **kwargs)

        feed.get_alt_data = blocked
        cache = AlternativeData(['FRED.GDP'], feed=feed, history_start='2020-01-01', fetch_timeout=2)
        waiter = asyncio.create_task(cache.refresh(cache.ids))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            worker = cache._inflight['FRED'].task
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert not worker.cancelled()
            retry = asyncio.create_task(cache.refresh(cache.ids))
            await asyncio.sleep(0)
            assert cache._inflight['FRED'].task is worker
        finally:
            release.set()
        assert await retry == {'FRED.GDP'}
        assert len(feed.calls) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['worker', 'base_exception', 'cancelled', 'publish'])
def test_provider_path_failure_preserves_other_results_and_backs_off(monkeypatch, failure):
    class ProviderFailure(BaseException):
        pass

    async def scenario():
        now = [100.0]
        cache = AlternativeData(['FRED.GDP', 'KALSHI.MARKET'], feed=Feed(),
                                history_start='2020-01-01', clock=lambda: now[0])
        await cache.refresh(cache.ids)
        old = cache.snapshot(['FRED.GDP'])
        original_fetch, original_publish = cache._fetch_provider, cache._publish
        attempts = []

        def fetch(ids, end):
            attempts.append(ids)
            if ids == ('FRED.GDP',):
                if failure == 'worker':
                    raise RuntimeError('worker failed')
                if failure == 'base_exception':
                    raise ProviderFailure('provider failed')
                if failure == 'cancelled':
                    raise asyncio.CancelledError()
            return original_fetch(ids, end)

        def publish(request):
            if failure == 'publish' and request.ids == ('FRED.GDP',):
                raise RuntimeError('publication failed')
            return original_publish(request)

        monkeypatch.setattr(cache, '_fetch_provider', fetch)
        monkeypatch.setattr(cache, '_publish', publish)
        assert await cache.refresh(cache.ids) == {'KALSHI.MARKET'}
        assert cache.snapshot(['FRED.GDP']) == old
        assert cache._inflight['FRED'].ready is None
        assert cache._retry_after['FRED.GDP'] == 160
        assert await cache.refresh(['FRED.GDP']) == set()
        assert attempts.count(('FRED.GDP',)) == 1
        now[0] += 60
        monkeypatch.setattr(cache, '_fetch_provider', original_fetch)
        monkeypatch.setattr(cache, '_publish', original_publish)
        assert await cache.refresh(['FRED.GDP']) == {'FRED.GDP'}
        assert 'FRED.GDP' not in cache._retry_after
    asyncio.run(scenario())


def test_publish_does_not_mark_failed_result_ready():
    async def scenario():
        cache = AlternativeData(['FRED.GDP'], feed=Feed(), history_start='2020-01-01')
        result = asyncio.get_running_loop().create_future()
        result.set_exception(RuntimeError('missing result'))
        request = _ProviderFetch(result, ('FRED.GDP',), 1000)
        with pytest.raises(RuntimeError, match='missing result'):
            cache._publish(request)
        assert request.ready is None
    asyncio.run(scenario())
