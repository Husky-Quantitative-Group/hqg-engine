import asyncio
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

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


@pytest.mark.parametrize("endpoint", [routes.stop_trading, routes.resume_trading, routes.liquidate_all])
def test_legacy_mutations_are_disabled(endpoint):
    session = make_session()
    with pytest.raises(HTTPException) as exc:
        run(endpoint(3, session))
    assert exc.value.status_code == 410
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


def test_legacy_portfolio_creation_is_disabled():
    session = make_session()
    with pytest.raises(HTTPException) as exc:
        run(routes.create_portfolio(routes.PortfolioRequest(portfolio_id=3, name="test", is_active=True), session))
    assert exc.value.status_code == 410
    session.commit.assert_not_awaited()


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
