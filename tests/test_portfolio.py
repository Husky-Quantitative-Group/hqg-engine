import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import pytest
from hqg_algorithms import Bar, BarSize, Hold, Liquidate, PortfolioView, TargetWeights
from src.portfolio import Portfolio


@pytest.fixture
def portfolio_view():
    return PortfolioView(equity=10_000, cash=10_000, positions={}, weights={})


class RecordingStrategy:
    def __init__(self, *outputs):
        self.outputs = list(outputs)
        self.calls = []

    def on_data(self, data, portfolio_view):
        self.calls.append((data, portfolio_view))
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return output


def make_portfolio(*strategy_specs):
    """Build a Portfolio without loading the application's strategy config."""
    portfolio = Portfolio.__new__(Portfolio)
    portfolio.config_path = "blank.yaml"
    portfolio.strategy_configs = []
    portfolio.strategies = [
        {
            "id": strategy_id,
            "instance": strategy,
            "weight": weight,
            "universe": list(universe),
            "cadence": SimpleNamespace(bar_size=bar_size),
        }
        for strategy_id, strategy, weight, universe, bar_size in strategy_specs
    ]
    portfolio._strategy_state = {}
    portfolio._bar_aggregate = {}
    return portfolio


def snapshot(timestamp, **values):
    return {"timestamp": timestamp, **values}


def run_on_data(portfolio, data, portfolio_view):
    return asyncio.run(portfolio.on_data(data, portfolio_view))


def test_load_config_reads_strategy_configuration(tmp_path):
    config_path = tmp_path / "portfolio.yaml"
    config_path.write_text(
        "strategies:\n"
        "  - id: momentum\n"
        "    class_name: Momentum\n"
        "    portfolio_weight: 0.6\n"
    )
    portfolio = Portfolio.__new__(Portfolio)
    portfolio.config_path = str(config_path)
    portfolio.strategy_configs = []

    portfolio.load_config()

    assert portfolio.strategy_configs == [
        {
            "id": "momentum",
            "class_name": "Momentum",
            "portfolio_weight": 0.6,
        }
    ]


def test_load_config_rejects_missing_file(tmp_path):
    portfolio = Portfolio.__new__(Portfolio)
    portfolio.config_path = str(tmp_path / "missing.yaml")
    portfolio.strategy_configs = []

    with pytest.raises(FileNotFoundError, match="Config file not found"):
        portfolio.load_config()


def test_load_config_rejects_configuration_without_strategies(tmp_path):
    config_path = tmp_path / "portfolio.yaml"
    config_path.write_text("strategies: []\n")
    portfolio = Portfolio.__new__(Portfolio)
    portfolio.config_path = str(config_path)
    portfolio.strategy_configs = []

    with pytest.raises(ValueError, match="No strategies configured"):
        portfolio.load_config()


def test_init_strategies_rejects_unknown_strategy_class():
    portfolio = Portfolio.__new__(Portfolio)
    portfolio.strategies = []
    portfolio.strategy_configs = [
        {
            "id": "unknown",
            "class_name": "MissingStrategy",
            "portfolio_weight": 1.0,
        }
    ]

    with pytest.raises(ValueError, match="Unknown strategy class: MissingStrategy"):
        portfolio.init_strategies()


def test_get_tickers_returns_unique_union_of_strategy_universes():
    portfolio = make_portfolio(
        ("first", RecordingStrategy(), 0.5, ["AAPL", "MSFT"], BarSize.DAILY),
        ("second", RecordingStrategy(), 0.5, ["MSFT", "GOOG"], BarSize.DAILY),
    )

    assert set(portfolio.get_tickers()) == {"AAPL", "MSFT", "GOOG"}


def test_create_bar_converts_numeric_strings():
    portfolio = make_portfolio()

    bar = portfolio._create_bar(
        {"open": "10", "high": "12.5", "low": "9", "close": "11", "volume": "42"}
    )

    assert bar == Bar(open=10.0, high=12.5, low=9.0, close=11.0, volume=42.0)


def test_create_bar_uses_price_and_close_for_missing_ohlc_values():
    portfolio = make_portfolio()

    bar = portfolio._create_bar(
        {"price": "101.25", "open": "invalid", "high": None, "low": None}
    )

    assert bar == Bar(
        open=101.25,
        high=101.25,
        low=101.25,
        close=101.25,
        volume=None,
    )


def test_create_bar_returns_none_without_valid_close_or_price():
    portfolio = make_portfolio()

    assert portfolio._create_bar({"close": "invalid", "price": None}) is None


@pytest.mark.parametrize(
    ("bar_size", "timestamp", "expected_period"),
    [
        (BarSize.DAILY, datetime(2026, 1, 2, tzinfo=timezone.utc), "2026-01-02"),
        (BarSize.WEEKLY, datetime(2026, 1, 2, tzinfo=timezone.utc), "2026-W01"),
        (BarSize.MONTHLY, datetime(2026, 5, 20, tzinfo=timezone.utc), "2026-05"),
        (BarSize.QUARTERLY, datetime(2026, 8, 20, tzinfo=timezone.utc), "2026-Q3"),
    ],
)
def test_on_data_tracks_the_current_cadence_period(
    portfolio_view, bar_size, timestamp, expected_period
):
    portfolio = make_portfolio(
        ("strategy", RecordingStrategy(), 1.0, ["AAPL"], bar_size)
    )

    result = run_on_data(
        portfolio,
        {"AAPL": snapshot(timestamp, close=100)},
        portfolio_view,
    )

    assert result == {}
    assert portfolio._strategy_state["strategy"]["current_bar_period"] == expected_period


# Portfolio.on_data() should roll a daily bar over when incoming data crosses into a new day.
def test_on_data_closes_aggregated_bar_and_starts_the_next_period(portfolio_view):
    strategy = RecordingStrategy(TargetWeights({"AAPL": 0.75}))
    portfolio = make_portfolio(
        ("momentum", strategy, 0.4, ["AAPL"], BarSize.DAILY)
    )
    first_day = datetime(2026, 1, 2, 14, tzinfo=timezone.utc)
    next_day = datetime(2026, 1, 3, 14, tzinfo=timezone.utc)

    assert run_on_data(
        portfolio,
        {
            "AAPL": snapshot(
                first_day, open=10, high=12, low=9, close=11, volume=100
            )
        },
        portfolio_view,
    ) == {}
    assert run_on_data(
        portfolio,
        {
            "AAPL": snapshot(
                first_day, open=11, high=13, low=8, close=12, volume=50
            )
        },
        portfolio_view,
    ) == {}

    result = run_on_data(
        portfolio,
        {
            "AAPL": snapshot(
                next_day, open=20, high=22, low=19, close=21, volume=25
            )
        },
        portfolio_view,
    )

    assert result == pytest.approx({"AAPL": 0.3})
    assert len(strategy.calls) == 1
    completed_slice, received_view = strategy.calls[0]
    assert received_view is portfolio_view
    assert completed_slice["AAPL"] == Bar(
        open=10.0,
        high=13.0,
        low=8.0,
        close=12.0,
        volume=150.0,
    )
    assert portfolio._strategy_state["momentum"] == {
        "current_bar_period": "2026-01-03",
        "last_output": {"AAPL": 0.75},
    }
    assert portfolio._bar_aggregate[
        ("momentum", BarSize.DAILY, "2026-01-03", "AAPL")
    ] == {
        "open": 20.0,
        "high": 22.0,
        "low": 19.0,
        "close": 21.0,
        "volume": 25.0,
    }


def test_on_data_hold_reuses_previous_target_and_liquidate_clears_it(portfolio_view):
    strategy = RecordingStrategy(TargetWeights({"SPY": 0.8}), Hold(), Liquidate())
    portfolio = make_portfolio(
        ("signals", strategy, 0.5, ["SPY"], BarSize.DAILY)
    )

    results = []
    for day in range(1, 5):
        timestamp = datetime(2026, 2, day, tzinfo=timezone.utc)
        results.append(
            run_on_data(
                portfolio,
                {"SPY": snapshot(timestamp, close=100 + day)},
                portfolio_view,
            )
        )

    assert results == [{}, {"SPY": 0.4}, {"SPY": 0.4}, {}]
    assert portfolio._strategy_state["signals"]["last_output"] == {}


def test_on_data_combines_dict_outputs_using_strategy_aum_weights(portfolio_view):
    first = RecordingStrategy({"AAPL": 0.5, "MSFT": 0.5})
    second = RecordingStrategy({"AAPL": 0.25, "GOOG": 0.75})
    portfolio = make_portfolio(
        ("first", first, 0.6, ["AAPL", "MSFT"], BarSize.DAILY),
        ("second", second, 0.4, ["AAPL", "GOOG"], BarSize.DAILY),
    )
    day_one = datetime(2026, 3, 1, tzinfo=timezone.utc)
    day_two = datetime(2026, 3, 2, tzinfo=timezone.utc)
    first_data = {
        symbol: snapshot(day_one, close=100)
        for symbol in ("AAPL", "MSFT", "GOOG")
    }
    second_data = {
        symbol: snapshot(day_two, close=101)
        for symbol in ("AAPL", "MSFT", "GOOG")
    }

    run_on_data(portfolio, first_data, portfolio_view)
    result = run_on_data(portfolio, second_data, portfolio_view)

    assert result == pytest.approx({"AAPL": 0.4, "MSFT": 0.3, "GOOG": 0.3})


def test_on_data_ignores_failed_strategy_and_continues_other_strategies(
    portfolio_view, caplog
):
    failed = RecordingStrategy(RuntimeError("strategy broke"))
    healthy = RecordingStrategy(TargetWeights({"MSFT": 1.0}))
    portfolio = make_portfolio(
        ("failed", failed, 0.5, ["AAPL"], BarSize.DAILY),
        ("healthy", healthy, 0.5, ["MSFT"], BarSize.DAILY),
    )
    day_one = datetime(2026, 4, 1, tzinfo=timezone.utc)
    day_two = datetime(2026, 4, 2, tzinfo=timezone.utc)

    run_on_data(
        portfolio,
        {
            "AAPL": snapshot(day_one, close=100),
            "MSFT": snapshot(day_one, close=200),
        },
        portfolio_view,
    )
    result = run_on_data(
        portfolio,
        {
            "AAPL": snapshot(day_two, close=101),
            "MSFT": snapshot(day_two, close=201),
        },
        portfolio_view,
    )

    assert result == pytest.approx({"MSFT": 0.5})
    assert "Strategy failed failed: strategy broke" in caplog.text
    assert portfolio._strategy_state["failed"]["last_output"] is None
