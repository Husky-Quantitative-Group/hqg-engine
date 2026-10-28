"""Decision-time alternative-data fetching; broker securities data stays separate."""

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
import logging
import math
import os
import re
import time
from types import MappingProxyType

import pandas as pd

logger = logging.getLogger(__name__)
FETCH_TIMEOUT_SECONDS = 120.0


@dataclass
class _ProviderFetch:
    task: asyncio.Task
    ids: tuple[str, ...]
    deadline: float
    expired: bool = False
    ready: set[str] | None = None


def normalize_series(ids):
    if not isinstance(ids, list):
        raise ValueError("Strategy.alt_data must be a list of PROVIDER.SERIES strings")
    result = []
    for value in ids:
        if not isinstance(value, str):
            raise ValueError("Alternative-data IDs must be strings")
        value = value.strip().upper()
        if not re.fullmatch(r"[A-Z0-9_-]+(?:\.[A-Z0-9_-]+)+", value):
            raise ValueError(f"Invalid alternative-data ID: {value!r}")
        if value not in result:
            result.append(value)
    return result


class AlternativeData:
    def __init__(self, ids, *, feed=None, history_start=None, clock=time.monotonic,
                 fetch_timeout=None):
        self.ids = normalize_series(ids)
        value = history_start if history_start is not None else os.getenv("ALT_DATA_HISTORY_START")
        try:
            self.start = datetime.combine(date.fromisoformat(value), datetime.min.time())
        except (TypeError, ValueError):
            raise ValueError("ALT_DATA_HISTORY_START must be a YYYY-MM-DD date") from None
        if self.start.date() > datetime.now(timezone.utc).date():
            raise ValueError("ALT_DATA_HISTORY_START cannot be in the future")
        try:
            self.fetch_timeout = float(fetch_timeout if fetch_timeout is not None else
                                       FETCH_TIMEOUT_SECONDS)
        except (TypeError, ValueError):
            raise ValueError("fetch_timeout must be a positive number of seconds") from None
        if not math.isfinite(self.fetch_timeout) or self.fetch_timeout <= 0:
            raise ValueError("fetch_timeout must be a positive number of seconds")
        if feed is None:
            from datafeed import Config, DataFeed

            feed = DataFeed(Config(fred_api_key=os.getenv("FRED_API_KEY")))
        self.feed = feed
        unsupported = {s.split(".", 1)[0] for s in self.ids} - set(feed.providers)
        if unsupported:
            raise ValueError(f"Unsupported alternative-data providers: {sorted(unsupported)}")
        self.histories = {}
        self.available_at = {}
        self.received_at = {}
        self._retry_after = {}
        self._clock = clock
        self._inflight: dict[str, _ProviderFetch] = {}

    @staticmethod
    def _prepare(frame):
        if frame.empty or not {"value", "available_at"} <= set(frame.columns):
            raise ValueError("Alternative history requires nonempty value/available_at columns")
        observations = pd.to_datetime(frame.index, utc=True, errors="raise")
        availability = pd.to_datetime(frame["available_at"], utc=True, errors="raise")
        if observations.isna().any() or availability.isna().any():
            raise ValueError("Alternative history contains missing timestamps")
        history, releases = {}, {}
        for observation, released, value in zip(observations, availability, frame["value"]):
            value = float(value)
            if not math.isfinite(value):
                raise ValueError("Alternative history contains non-finite values")
            # Slice/backtester histories use timezone-naive UTC observation dates.
            observation = observation.tz_localize(None).to_pydatetime()
            released = released.to_pydatetime()
            # Only use availability to choose the newest revision of a duplicate date.
            if observation in releases and released < releases[observation]:
                continue
            history[observation] = value
            releases[observation] = released
        # Expose observations chronologically; do not sort rows by availability.
        history = dict(sorted(history.items()))
        releases = {observation: releases[observation] for observation in history}
        return MappingProxyType(history), MappingProxyType(releases)

    def _fetch_provider(self, ids, end):
        # DataFeed's public batch API raises on any failed series. Separate calls
        # preserve partial success without changing that API's strict contract.
        results = {}
        for series in ids:
            try:
                results[series] = self.feed.get_alt_data([series], start=self.start, end=end)[series]
            except Exception as exc:
                results[series] = exc
        return results

    def _expire(self, provider, request):
        if not request.expired:
            request.expired = True
            for series in request.ids:
                self._retry_after[series] = self._clock() + 60
            logger.warning("Alternative-data fetch timed out for %s after %ss; "
                           "retaining worker until it exits", provider, self.fetch_timeout)

    def _publish(self, request):
        if request.ready is not None:
            return request.ready
        frames = request.task.result()
        ready = set()
        received = datetime.now(timezone.utc)
        for series in request.ids:
            try:
                frame = frames[series]
                if isinstance(frame, Exception):
                    raise frame
                history, releases = self._prepare(frame)
            except Exception:
                self._retry_after[series] = self._clock() + 60
                logger.exception("Alternative history unavailable for %s; retry in 60s", series)
                continue
            self.histories[series] = history
            self.available_at[series] = releases
            self.received_at[series] = received
            self._retry_after.pop(series, None)
            ready.add(series)
        logger.info("Refreshed alternative series %s at %s", sorted(ready), received.isoformat())
        request.ready = ready
        return ready

    async def _refresh_provider(self, provider, ids):
        try:
            return await self._await_provider(provider, ids)
        except asyncio.CancelledError:
            # Cancellation of refresh/shutdown must propagate. A worker that
            # cancelled itself is a provider failure, not caller cancellation.
            if asyncio.current_task().cancelling():
                raise
        except (KeyboardInterrupt, SystemExit, GeneratorExit):
            raise
        except BaseException:
            logger.exception("Alternative-data provider %s failed; retry in 60s", provider)

        request = self._inflight.get(provider)
        failed_ids = set(ids)
        if request is not None:
            failed_ids.update(request.ids)
            request.expired = True
        for series in failed_ids:
            self._retry_after[series] = self._clock() + 60
        return set()

    async def _await_provider(self, provider, ids):
        loop = asyncio.get_running_loop()
        request = self._inflight.get(provider)
        if request is not None and request.task.done() and (
                request.expired or request.ready is not None):
            del self._inflight[provider]
            request = None
        if request is None:
            end = datetime.now(timezone.utc).replace(tzinfo=None)
            task = asyncio.create_task(asyncio.to_thread(self._fetch_provider, tuple(ids), end))
            # Retrieve late errors even if all waiters timed out or were cancelled.
            task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
            request = _ProviderFetch(task, tuple(ids), loop.time() + self.fetch_timeout)
            # No await before registration: concurrent callers share one worker/provider.
            self._inflight[provider] = request
        if request.expired:
            return set()
        remaining = request.deadline - loop.time()
        if remaining <= 0:
            self._expire(provider, request)
            return set()
        try:
            # Shield keeps the worker tracked after a caller timeout/cancellation.
            await asyncio.wait_for(asyncio.shield(request.task), timeout=remaining)
        except asyncio.TimeoutError:
            self._expire(provider, request)
            return set()
        if request.expired or loop.time() > request.deadline:
            self._expire(provider, request)
            return set()
        return self._publish(request) & set(ids)

    async def refresh(self, ids):
        """Fetch providers concurrently, with one worker and deadline per provider."""
        groups = defaultdict(list)
        for series in sorted(set(ids)):
            if self._clock() >= self._retry_after.get(series, 0):
                groups[series.split(".", 1)[0]].append(series)
        results = await asyncio.gather(*(
            self._refresh_provider(provider, series_ids)
            for provider, series_ids in groups.items()
        ))
        return set().union(*results)

    def snapshot(self, ids):
        return MappingProxyType({series: self.histories[series] for series in ids})
