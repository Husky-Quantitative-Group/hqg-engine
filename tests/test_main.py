import logging
import sys

import pytest

from src.main import build_portfolio_view, setup_logging, truncate


def test_truncate_keeps_three_decimal_places_without_rounding():
    assert truncate(1.2349) == 1.234
    assert truncate(-1.2349) == -1.234


def test_build_portfolio_view_calculates_cash_and_weights():
    positions = {"AAPL": 2, "MSFT": "1.5"}
    market_data = {
        "AAPL": {"close": "100"},
        "MSFT": {"price": 200},
    }

    view = build_portfolio_view(1_000, positions, market_data)

    assert view.equity == 1_000.0
    assert view.cash == 500.0
    assert view.positions == {"AAPL": 2.0, "MSFT": 1.5}
    assert view.weights == pytest.approx({"AAPL": 0.2, "MSFT": 0.3})


def test_build_portfolio_view_prefers_close_over_price():
    view = build_portfolio_view(
        1_000,
        {"AAPL": 2},
        {"AAPL": {"close": 100, "price": 400}},
    )

    assert view.cash == 800.0
    assert view.weights == pytest.approx({"AAPL": 0.2})


def test_build_portfolio_view_ignores_missing_or_invalid_prices():
    view = build_portfolio_view(
        1_000,
        {"AAPL": 2, "MSFT": 3, "GOOG": 4},
        {"AAPL": {"close": "invalid", "price": None}, "MSFT": None},
    )

    assert view.cash == 1_000.0
    assert view.weights == {}
    assert view.positions == {"AAPL": 2.0, "MSFT": 3.0, "GOOG": 4.0}


def test_build_portfolio_view_clamps_negative_cash_to_zero():
    view = build_portfolio_view(100, {"AAPL": 2}, {"AAPL": {"close": 75}})

    assert view.cash == 0.0
    assert view.weights == pytest.approx({"AAPL": 1.5})


@pytest.mark.parametrize("equity", [0, -100])
def test_build_portfolio_view_handles_non_positive_equity(equity):
    view = build_portfolio_view(
        equity, {"AAPL": "2"}, {"AAPL": {"close": 100}}
    )

    assert view.equity == float(equity)
    assert view.cash == 0.0
    assert view.positions == {"AAPL": 2.0}
    assert view.weights == {}


def test_setup_logging_splits_stdout_and_stderr_by_level(monkeypatch):
    calls = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))

    setup_logging()

    (config,) = calls
    assert config["level"] == logging.INFO
    assert config["format"] == "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    out, err = config["handlers"]
    assert out.stream is sys.stdout
    assert err.stream is sys.stderr
    assert err.level == logging.WARNING

    def record(level):
        return logging.LogRecord("test", level, __file__, 0, "message", None, None)

    assert out.filter(record(logging.INFO))
    assert out.filter(record(logging.DEBUG))
    assert not out.filter(record(logging.WARNING))
    assert not out.filter(record(logging.ERROR))
