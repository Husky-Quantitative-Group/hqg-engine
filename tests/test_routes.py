import asyncio
import sys
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pandas as pd
import pytest
from fastapi import HTTPException

from src import routes
from src.db.models import Action


class ScalarValues:
    def __init__(self, values):
        self.values = list(values)

    def all(self):
        return self.values

    def first(self):
        return self.values[0] if self.values else None


class Result:
    def __init__(self, values=(), scalar=None, rows=None):
        self.values = list(values)
        self.scalar = scalar
        self.rows = list(rows or [])

    def scalar_one_or_none(self):
        return self.scalar

    def scalars(self):
        return ScalarValues(self.values)

    def all(self):
        return self.rows


def run(coroutine):
    return asyncio.run(coroutine)


def make_session(*results):
    session = Mock()
    session.execute = AsyncMock(side_effect=results)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    return session


class FixedDate(date):
    @classmethod
    def today(cls):
        return cls(2026, 9, 1)


@pytest.mark.parametrize(
    ("timeframe", "expected"),
    [
        (None, None),
        (routes.Timeframe.THREE_MONTHS, date(2026, 6, 3)),
        (routes.Timeframe.SIX_MONTHS, date(2026, 3, 5)),
        (routes.Timeframe.YEAR_TO_DATE, date(2026, 1, 1)),
    ],
)
def test_timeframe_to_date_range(monkeypatch, timeframe, expected):
    monkeypatch.setattr(routes, "date", FixedDate)

    assert routes.timeframe_to_date_range(timeframe) == expected


def test_get_portfolio_returns_database_model():
    portfolio = SimpleNamespace(portfolio_id=7, name="Growth", is_active=True)
    session = make_session(Result(scalar=portfolio))

    assert run(routes.get_portfolio(7, session)) is portfolio


def test_get_portfolio_raises_404_when_missing():
    session = make_session(Result(scalar=None))

    with pytest.raises(HTTPException) as exc_info:
        run(routes.get_portfolio(404, session))

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Portfolio 404 not found"


@pytest.mark.parametrize(
    "path", ["/portfolio", "/portfolio/3/stop", "/portfolio/3/resume", "/portfolio/3/liquidate"]
)
def test_legacy_portfolio_mutations_are_removed(path):
    from src.server import app

    async def scenario():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as client:
            assert (await client.post(path, json={})).status_code == 404

    run(scenario())


def test_get_equity_serializes_snapshots_in_query_order():
    portfolio = SimpleNamespace(portfolio_id=1)
    snapshots = [
        SimpleNamespace(as_of=date(2026, 1, 1), equity="1000.25"),
        SimpleNamespace(as_of=date(2026, 1, 2), equity=1010),
    ]
    session = make_session(Result(scalar=portfolio), Result(values=snapshots))

    response = run(routes.get_equity(1, routes.Timeframe.YEAR_TO_DATE, session))

    assert [point.model_dump() for point in response.data] == [
        {"timestamp": "2026-01-01", "equity_value": 1000.25},
        {"timestamp": "2026-01-02", "equity_value": 1010.0},
    ]


def test_get_snapshot_calculates_capital_profit_and_return():
    portfolio = SimpleNamespace(portfolio_id=1)
    latest = SimpleNamespace(as_of=date(2026, 1, 3), equity=1_200)
    previous = SimpleNamespace(as_of=date(2026, 1, 2), equity=1_100)
    baseline = SimpleNamespace(as_of=date(2026, 1, 1), equity=1_000)
    latest_holdings = [SimpleNamespace(market_value=700), SimpleNamespace(market_value=200)]
    previous_holdings = [SimpleNamespace(market_value=800)]
    session = make_session(
        Result(scalar=portfolio),
        Result(values=[latest, previous]),
        Result(values=[baseline]),
        Result(values=latest_holdings),
        Result(values=previous_holdings),
    )

    response = run(routes.get_snapshot(1, routes.Timeframe.YEAR_TO_DATE, session))

    assert [item.model_dump() for item in response.snapshots] == [
        {
            "equity": 1200.0,
            "capital": 900.0,
            "net_profit": 200.0,
            "return_pct": 0.2,
            "as_of": "2026-01-03",
        },
        {
            "equity": 1100.0,
            "capital": 800.0,
            "net_profit": 100.0,
            "return_pct": 0.1,
            "as_of": "2026-01-02",
        },
    ]


def test_get_metrics_returns_zeroes_with_too_few_snapshots():
    portfolio = SimpleNamespace(portfolio_id=1)
    session = make_session(
        Result(scalar=portfolio),
        Result(values=[SimpleNamespace(as_of=date(2026, 1, 1), equity=100)]),
    )

    response = run(routes.get_metrics(1, routes.Timeframe.YEAR_TO_DATE, session))

    assert response.metrics == {
        "sharpe": 0.0,
        "sortino": 0.0,
        "cagr": 0.0,
        "max_drawdown": 0.0,
        "alpha": 0.0,
        "beta": 0.0,
        "std": 0.0,
    }


def test_get_strategy_allocations_uses_only_most_recent_snapshot():
    portfolio = SimpleNamespace(portfolio_id=1)
    recent = date(2026, 1, 3)
    snapshots = [
        SimpleNamespace(as_of=recent, strategy_name="momentum", weight="0.6"),
        SimpleNamespace(as_of=recent, strategy_name="value", weight="0.4"),
        SimpleNamespace(as_of=date(2026, 1, 2), strategy_name="old", weight=1),
    ]
    session = make_session(Result(scalar=portfolio), Result(values=snapshots))

    response = run(routes.get_strategy_allocations(1, session))

    assert response.allocations == {"momentum": 0.6, "value": 0.4}


def test_get_asset_allocations_uses_latest_performance_snapshot():
    portfolio = SimpleNamespace(portfolio_id=1)
    latest = SimpleNamespace(as_of=date(2026, 1, 3))
    holdings = [
        (SimpleNamespace(quantity="2.5", market_value="250.75"), "AAPL"),
        (SimpleNamespace(quantity=1, market_value=300), "MSFT"),
    ]
    session = make_session(
        Result(scalar=portfolio), Result(values=[latest]), Result(rows=holdings)
    )

    response = run(routes.get_asset_allocations(1, None, session))

    assert response.allocations == {
        "AAPL": {"quantity": 2.5, "market_value": 250.75},
        "MSFT": {"quantity": 1.0, "market_value": 300.0},
    }


def test_get_execution_events_serializes_action_and_quantity():
    portfolio = SimpleNamespace(portfolio_id=1)
    events = [
        SimpleNamespace(
            action=Action.BUY,
            symbol="AAPL",
            quantity="1.25",
            timestamp=datetime(2026, 1, 3, 14, 30),
        )
    ]
    session = make_session(Result(scalar=portfolio), Result(values=events))

    response = run(
        routes.get_execution_events(1, routes.Timeframe.YEAR_TO_DATE, session)
    )

    assert [event.model_dump() for event in response.events] == [
        {
            "action": "buy",
            "symbol": "AAPL",
            "quantity": 1.25,
            "timestamp": "2026-01-03T14:30:00",
        }
    ]


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ({"AAPL": 0.75, "MSFT": "0.25"}, {"AAPL": 0.75, "MSFT": 0.25}),
        ({"allocations": {"AAPL": {"weight": "0.8"}}}, {"AAPL": 0.8}),
    ],
)
def test_get_allocation_events_supports_current_and_legacy_shapes(stored, expected):
    portfolio = SimpleNamespace(portfolio_id=1)
    row = SimpleNamespace(
        timestamp=datetime(2026, 1, 3, 9), allocations=stored
    )
    session = make_session(Result(scalar=portfolio), Result(values=[row]))

    response = run(routes.get_allocation_events(1, None, session))

    assert response.events[0].timestamp == "2026-01-03T09:00:00"
    assert response.events[0].allocations.allocations == expected


def curve(points):
    return [(point.timestamp, point.equity_value) for point in points]


def test_replay_rebalances_at_closes_and_drifts_between_signals():
    closes = [
        (date(2026, 11, 9), {"SPY": 100.0}),
        (date(2026, 11, 10), {"SPY": 110.0}),
        (date(2026, 11, 11), {"SPY": 120.0}),
        (date(2026, 11, 12), {"SPY": 60.0}),
    ]
    # Sunday's targets take effect at Monday's close.
    signals = {date(2026, 11, 8): {"SPY": 0.5}, date(2026, 11, 11): {"SPY": 1.0}}

    assert curve(routes.replay(signals, closes)) == [
        ("2026-11-09", 100_000.0),
        ("2026-11-10", 105_000.0),
        ("2026-11-11", 110_000.0),
        ("2026-11-12", 55_000.0),
    ]


def target(timestamp, strategy_id, allocation, weights, *, bar_timestamp=None,
           version=1, source_digest="source-1"):
    return SimpleNamespace(
        timestamp=timestamp, strategy_id=strategy_id, allocation=allocation,
        target_weights=weights, bar_timestamp=bar_timestamp or timestamp,
        version=version, source_digest=source_digest,
    )


def test_get_replay_combines_strategies_by_allocation(monkeypatch):
    portfolio = SimpleNamespace(portfolio_id=1, mode="paper")
    monday, late_tuesday, wednesday = (
        datetime(2026, 11, 9, 21, 1),  # 4:01 PM in New York
        datetime(2026, 11, 11, 2),  # 9 PM on Tuesday in New York
        datetime(2026, 11, 11, 21, 1),
    )
    rows = [
        target(monday, "a", 1.0, {"SPY": 1.0}),
        target(late_tuesday, "a", 0.5, {"SPY": 1.0}),
        target(late_tuesday, "b", 0.5, {"TLT": 1.0}),
        target(wednesday, "b", 1.0, {"TLT": 1.0}),  # "a" left the portfolio.
    ]
    closes = [
        (date(2026, 11, 9), {"SPY": 100.0, "TLT": 50.0}),
        (date(2026, 11, 10), {"SPY": 110.0, "TLT": 50.0}),
        (date(2026, 11, 11), {"SPY": 121.0, "TLT": 55.0}),
        (date(2026, 11, 12), {"SPY": 100.0, "TLT": 60.0}),
    ]
    requested = []
    monkeypatch.setattr(
        routes, "daily_closes", lambda *args: requested.append(args) or closes
    )
    session = make_session(Result(scalar=portfolio), Result(values=rows))

    response = run(routes.get_replay(1, session))

    assert requested == [(["SPY", "TLT"], date(2026, 11, 9))]
    assert response.mode == "paper"
    assert curve(response.total) == [
        ("2026-11-09", 100_000.0),
        ("2026-11-10", 110_000.0),
        ("2026-11-11", 121_000.0),
        ("2026-11-12", 132_000.0),
    ]
    assert [(item.strategy_id, curve(item.data)) for item in response.strategies] == [
        ("a", [
            ("2026-11-09", 100_000.0),
            ("2026-11-10", 110_000.0),
            ("2026-11-11", 121_000.0),
            ("2026-11-12", 121_000.0),
        ]),
        ("b", [
            ("2026-11-10", 100_000.0),
            ("2026-11-11", 110_000.0),
            ("2026-11-12", 120_000.0),
        ]),
    ]


def test_get_replay_is_empty_without_logged_targets(monkeypatch):
    monkeypatch.setattr(routes, "daily_closes", Mock(side_effect=AssertionError))
    session = make_session(Result(scalar=SimpleNamespace(portfolio_id=1)), Result())

    response = run(routes.get_replay(1, session))

    assert response.total == [] and response.strategies == []


def test_get_replay_reports_unavailable_prices(monkeypatch):
    monkeypatch.setattr(routes, "daily_closes", Mock(side_effect=ValueError("no data")))
    rows = [target(datetime(2026, 11, 9, 21, 1), "a", 1.0, {"SPY": 1.0})]
    session = make_session(Result(scalar=SimpleNamespace(portfolio_id=1)), Result(values=rows))

    with pytest.raises(HTTPException) as exc_info:
        run(routes.get_replay(1, session))

    assert exc_info.value.status_code == 502
    assert exc_info.value.detail == "Price data unavailable: no data"


def test_get_replay_preserves_history_before_a_later_ticker_exists(monkeypatch):
    days = pd.to_datetime(["2026-11-09", "2026-11-10", "2026-11-11"])
    frame = pd.DataFrame(
        {("SPY", "close"): [100.0, 110.0, 120.0],
         ("NEW", "close"): [float("nan"), float("nan"), 50.0]},
        index=days,
    )
    frame.columns.names = ["symbol", "field"]
    feed = Mock()
    feed.get_securities.return_value = frame
    monkeypatch.setitem(sys.modules, "datafeed", SimpleNamespace(
        Config=Mock(), DataFeed=Mock(return_value=feed),
    ))
    rows = [
        target(datetime(2026, 11, 9, 21, 1), "a", 1.0, {"SPY": 1.0}),
        target(datetime(2026, 11, 11, 21, 1), "b", 1.0, {"NEW": 1.0}),
    ]
    session = make_session(Result(scalar=SimpleNamespace(portfolio_id=1)), Result(values=rows))

    response = run(routes.get_replay(1, session))

    assert curve(response.total) == [
        ("2026-11-09", 100_000.0),
        ("2026-11-10", 110_000.0),
        ("2026-11-11", 120_000.0),
    ]
    assert curve(response.strategies[1].data) == [("2026-11-11", 100_000.0)]


@pytest.mark.parametrize("fresh_signal, expected", [
    (None, 100_000.0), ("bar", 112_500.0), ("version", 112_500.0), ("source", 112_500.0),
])
def test_get_replay_distinguishes_held_targets_from_fresh_identical_targets(
    monkeypatch, fresh_signal, expected,
):
    monday, tuesday = datetime(2026, 11, 9, 21, 1), datetime(2026, 11, 10, 21, 1)
    rows = [
        target(monday, "a", 0.5, {"SPY": 0.5}),
        target(monday, "b", 0.5, {}),
        target(tuesday, "a", 0.5, {"SPY": 0.5},
               bar_timestamp=tuesday if fresh_signal == "bar" else monday,
               version=2 if fresh_signal == "version" else 1,
               source_digest="source-2" if fresh_signal == "source" else "source-1"),
        target(tuesday, "b", 0.5, {"TLT": 1.0}),
    ]
    closes = [
        (date(2026, 11, 9), {"SPY": 100.0, "TLT": 50.0}),
        (date(2026, 11, 10), {"SPY": 200.0, "TLT": 50.0}),
        (date(2026, 11, 11), {"SPY": 100.0, "TLT": 50.0}),
    ]
    monkeypatch.setattr(routes, "daily_closes", lambda *args: closes)
    session = make_session(Result(scalar=SimpleNamespace(portfolio_id=1)), Result(values=rows))

    response = run(routes.get_replay(1, session))

    assert curve(response.strategies[0].data) == [
        ("2026-11-09", 100_000.0),
        ("2026-11-10", 150_000.0),
        ("2026-11-11", expected),
    ]
    # Portfolio targets still rebalance when another strategy trades.
    assert curve(response.total) == [
        ("2026-11-09", 100_000.0),
        ("2026-11-10", 125_000.0),
        ("2026-11-11", 109_375.0),
    ]


def test_get_replay_reinvests_when_a_removed_strategy_returns_with_the_same_bar(monkeypatch):
    monday = datetime(2026, 11, 9, 21, 1)
    rows = [
        target(monday, "a", 1.0, {"SPY": 0.5}),
        target(datetime(2026, 11, 10, 21, 1), "b", 1.0, {}),
        target(datetime(2026, 11, 11, 21, 1), "a", 1.0, {"SPY": 0.5}, bar_timestamp=monday),
    ]
    closes = [
        (date(2026, 11, 9), {"SPY": 100.0}),
        (date(2026, 11, 10), {"SPY": 200.0}),
        (date(2026, 11, 11), {"SPY": 100.0}),
        (date(2026, 11, 12), {"SPY": 200.0}),
    ]
    monkeypatch.setattr(routes, "daily_closes", lambda *args: closes)
    session = make_session(Result(scalar=SimpleNamespace(portfolio_id=1)), Result(values=rows))

    response = run(routes.get_replay(1, session))

    assert curve(response.strategies[0].data) == [
        ("2026-11-09", 100_000.0), ("2026-11-10", 150_000.0),
        ("2026-11-11", 150_000.0), ("2026-11-12", 225_000.0),
    ]


@pytest.mark.parametrize("price", [None, 0.0, -1.0, float("nan"), float("inf")])
@pytest.mark.parametrize("already_held", [False, True])
def test_replay_rejects_unusable_prices_only_when_needed(price, already_held):
    monday, tuesday = date(2026, 11, 9), date(2026, 11, 10)
    signals = {monday if already_held else tuesday: {"SPY": 1.0}}
    closes = [(monday, {"SPY": 100.0}), (tuesday, {} if price is None else {"SPY": price})]

    with pytest.raises(ValueError, match="SPY.*2026-11-10"):
        routes.replay(signals, closes)


def test_replay_does_not_require_prices_for_zero_weight_targets():
    monday, tuesday = date(2026, 11, 9), date(2026, 11, 10)

    assert curve(routes.replay({monday: {"UNUSED": 0.0}}, [(monday, {}), (tuesday, {})])) == [
        ("2026-11-09", 100_000.0), ("2026-11-10", 100_000.0),
    ]


def test_get_replay_reports_missing_required_price(monkeypatch):
    monkeypatch.setattr(routes, "daily_closes", lambda *args: [(date(2026, 11, 9), {})])
    rows = [target(datetime(2026, 11, 9, 21, 1), "a", 1.0, {"SPY": 1.0})]
    session = make_session(Result(scalar=SimpleNamespace(portfolio_id=1)), Result(values=rows))

    with pytest.raises(HTTPException) as exc_info:
        run(routes.get_replay(1, session))

    assert exc_info.value.status_code == 502
    assert "SPY" in exc_info.value.detail and "2026-11-09" in exc_info.value.detail
