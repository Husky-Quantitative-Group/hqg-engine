"""Resolve completed stock-bar periods using the exchange's session calendar."""

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from hqg_algorithms import BarSize, ExecutionTiming

MARKET_ZONE = ZoneInfo("America/New_York")
# Allow the closing auction/bar to be published before reading a completed bar.
PUBLICATION_DELAY = timedelta(minutes=1)


def period_start(day, bar_size):
    bar_size = BarSize(bar_size)
    if bar_size == BarSize.DAILY:
        return day
    if bar_size == BarSize.WEEKLY:
        return day - timedelta(days=day.weekday())
    month = day.month if bar_size == BarSize.MONTHLY else (day.month - 1) // 3 * 3 + 1
    return day.replace(month=month, day=1)


def completed_period(calendar, bar_size, execution, now):
    """Return the latest finished period, or None while awaiting its next open."""
    execution = ExecutionTiming(execution)
    sessions = sorted(calendar, key=lambda session: session.date)
    periods = {}
    for index, session in enumerate(sessions):
        start = period_start(session.date, bar_size)
        close = session.close.replace(tzinfo=MARKET_ZONE)
        next_open = (
            sessions[index + 1].open.replace(tzinfo=MARKET_ZONE)
            if index + 1 < len(sessions) else None
        )
        periods[start] = (close, next_open)

    for start, (close, next_open) in reversed(periods.items()):
        if close + PUBLICATION_DELAY > now:
            continue
        if execution == ExecutionTiming.CLOSE_TO_NEXT_OPEN and (
            next_open is None or next_open > now
        ):
            return None
        return datetime.combine(start, time(), MARKET_ZONE), close + PUBLICATION_DELAY
    return None
