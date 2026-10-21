from datetime import date, datetime, timezone

import pytest
from alpaca.trading.models import Calendar

from src.control.cadence import completed_period, period_start
from src.control.sandbox import Sandbox


def session(day, close="16:00"):
    return Calendar(date=day, open="09:30", close=close)


@pytest.mark.parametrize("day,size,expected", [
    ("2026-10-21", "1d", "2026-10-21"),
    ("2027-01-01", "1w", "2026-12-28"),
    ("2026-10-21", "1m", "2026-10-01"),
    ("2026-12-31", "1q", "2026-10-01"),
])
def test_periods_use_calendar_boundaries(day, size, expected):
    assert period_start(date.fromisoformat(day), size).isoformat() == expected


@pytest.mark.parametrize("size", ["1d", "1w", "1m", "1q"])
def test_current_period_is_not_eligible_before_its_final_session_closes(size):
    calendar = [session("2026-09-25"), session("2026-09-30"), session("2026-10-01"),
                session("2026-10-02"), session("2026-10-30"), session("2026-12-31")]
    now = datetime.fromisoformat("2026-10-02T15:00:00-04:00")
    start, end = completed_period(calendar, size, "close_to_close", now)
    assert start.date() < period_start(now.date(), size)
    assert end < now


def test_holiday_and_early_close_determine_week_completion_and_next_open():
    # Friday July 3 is a holiday; Thursday's early close ends the trading week.
    calendar = [session("2026-06-26"), session("2026-07-02", "13:00"), session("2026-07-06")]
    before = datetime.fromisoformat("2026-07-02T13:00:30-04:00")
    after = datetime.fromisoformat("2026-07-02T13:01:00-04:00")
    assert completed_period(calendar, "1w", "close_to_close", before)[0].date() == date(2026, 6, 22)
    assert completed_period(calendar, "1w", "close_to_close", after)[0].date() == date(2026, 6, 29)
    assert completed_period(calendar, "1w", "close_to_next_open", after) is None
    assert completed_period(calendar, "1w", "close_to_next_open", datetime.fromisoformat(
        "2026-07-06T09:30:00-04:00"
    ))[0].date() == date(2026, 6, 29)


def test_next_open_respects_daylight_saving_time():
    calendar = [session("2026-03-06"), session("2026-03-09")]
    assert completed_period(calendar, "1d", "close_to_next_open",
                            datetime(2026, 3, 9, 13, 29, tzinfo=timezone.utc)) is None
    assert completed_period(calendar, "1d", "close_to_next_open",
                            datetime(2026, 3, 9, 13, 30, tzinfo=timezone.utc))[0].date() == date(2026, 3, 6)


@pytest.mark.parametrize("declaration,size,execution", [
    ("", "1d", "close_to_close"),
    ("cadence = Cadence(bar_size=BarSize.WEEKLY, execution=ExecutionTiming.CLOSE_TO_NEXT_OPEN)",
     "1w", "close_to_next_open"),
])
def test_sandbox_reads_cadence_without_executing_strategy_source(declaration, size, execution):
    sandbox = Sandbox({
        "strategy_id": "cadence", "version": 1, "entrypoint": "main.py",
        "files": {"main.py": f"""from hqg_algorithms import Strategy, Cadence, BarSize, ExecutionTiming
raise AssertionError('Source must not execute on the host')
class Test(Strategy):
    universe = ["AAPL"]
    {declaration}
    def on_data(self, data, portfolio): pass
"""},
    })
    assert sandbox.cadence.bar_size.value == size
    assert sandbox.cadence.execution.value == execution
