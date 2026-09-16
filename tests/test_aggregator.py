import pytest
from src.aggregator import aggregate_allocations


def test_empty_allocations_return_empty_portfolio():
    assert aggregate_allocations([]) == {}


def test_single_strategy_allocations_are_scaled_by_aum_weight():
    allocations = [
        ("momentum", [("AAPL", 0.6), ("MSFT", 0.4)], 0.25),
    ]

    result = aggregate_allocations(allocations)

    assert result == pytest.approx({"AAPL": 0.15, "MSFT": 0.10})


def test_overlapping_tickers_are_combined_across_strategies():
    allocations = [
        ("momentum", [("AAPL", 0.5), ("MSFT", 0.5)], 0.6),
        ("value", [("AAPL", 0.25), ("GOOG", 0.75)], 0.4),
    ]

    result = aggregate_allocations(allocations)

    assert result == pytest.approx(
        {
            "AAPL": 0.4,
            "MSFT": 0.3,
            "GOOG": 0.3,
        }
    )


def test_duplicate_tickers_within_a_strategy_are_combined():
    allocations = [
        ("split_orders", [("AAPL", 0.2), ("AAPL", 0.3)], 0.5),
    ]

    assert aggregate_allocations(allocations) == pytest.approx({"AAPL": 0.25})


def test_underallocated_strategy_is_not_renormalized():
    allocations = [
        ("defensive", [("BND", 0.3), ("GLD", 0.2)], 0.8),
    ]

    result = aggregate_allocations(allocations)

    assert result == pytest.approx({"BND": 0.24, "GLD": 0.16})
    assert sum(result.values()) == pytest.approx(0.4)


def test_strategy_allocations_summing_to_one_are_allowed():
    allocations = [
        ("balanced", [("SPY", 0.5), ("BND", 0.5)], 1.0),
    ]

    assert aggregate_allocations(allocations) == pytest.approx(
        {"SPY": 0.5, "BND": 0.5}
    )


def test_strategy_allocations_exceeding_one_raise_value_error():
    allocations = [
        ("leveraged", [("SPY", 0.7), ("QQQ", 0.4)], 0.5),
    ]

    with pytest.raises(
        ValueError,
        match=r"^leveraged allocations sum to 1\.1 \(exceeds 1\.0\)$",
    ):
        aggregate_allocations(allocations)
