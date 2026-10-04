"""Approved deterministic allocation strategy for the mock vertical slice."""


class MockBalanced:
    universe = ("AAPL", "MSFT")

    def __init__(self):
        self.cycles = 0

    def on_data(self, prices):
        if set(prices) != set(self.universe) or any(price <= 0 for price in prices.values()):
            raise ValueError("Complete positive market data required")

        self.cycles += 1

        return {symbol: 0.4 for symbol in self.universe}
