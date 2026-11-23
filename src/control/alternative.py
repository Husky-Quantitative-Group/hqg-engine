"""Alternative-data access in the network-enabled provider process."""

from datetime import datetime, timedelta, timezone
import logging
from math import ceil

from hqg_algorithms import get_strategy_metadata

from src.alt_data import AlternativeData, normalize_series
from src.control.cadence import MARKET_ZONE
from src.control.packages import declared_lookback, validate_package

logger = logging.getLogger(__name__)


def lookback_start(count, today):
    """Subtract N weekdays plus a rounded-up 10% buffer for holidays etc."""
    remaining = ceil(count * 110 / 100)
    while remaining:
        today -= timedelta(days=1)
        if today.weekday() < 5:
            remaining -= 1
    return today


class AlternativeService:
    def __init__(self):
        self.cache = None

    async def fetch(self, state, mode, ids):
        requested = normalize_series(ids)
        lookbacks = {}
        declared = set()
        configuration = state['configuration'] or {}
        for item in configuration.get('modes', {}).get(mode, {}).get('strategies', []):
            package = state['strategy_packages'][item['source_digest']]
            metadata = get_strategy_metadata(package['files'][package['entrypoint']])
            series = normalize_series(getattr(metadata, 'alt_data', []))
            if not series:
                continue
            declared.update(series)
            _, package_metadata = validate_package(package)
            count = declared_lookback(package['files'][package['entrypoint']], package_metadata['class_name'])
            count = 1 if count is None else count
            if type(count) is not int or not 1 <= count <= 1000:
                logger.warning("Skipping alternative history for %s strategy %s: "
                               "lookback must be an integer from 1 to 1000 (got %r)",
                               mode, item['strategy_id'], count)
                continue
            for series_id in series:
                lookbacks[series_id] = max(lookbacks.get(series_id, 1), count)
        if not requested or not set(requested) <= declared:
            raise ValueError("Alternative series must be declared by configured strategies")
        requested = [series for series in requested if series in lookbacks]
        if not requested:
            return {'ready': [], 'history': {}}
        today = datetime.now(timezone.utc).astimezone(MARKET_ZONE).date()
        dates = {count: lookback_start(count, today)
                 for count in {lookbacks[series] for series in requested}}
        starts = {series: dates[lookbacks[series]].isoformat() for series in requested}
        if self.cache is None:
            self.cache = AlternativeData(requested, history_start=min(starts.values()))
        ready = await self.cache.refresh(requested, starts=starts)
        history = self.cache.records(ready)
        return {'ready': sorted(ready), 'history': history}
