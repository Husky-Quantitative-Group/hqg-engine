from datetime import timedelta
from hqg_algorithms import Strategy, Cadence, Slice, PortfolioView
from collections import deque


class SMA_AAPL(Strategy):
    def __init__(self):
        self.window = deque()
        self.aapl = "AAPL"
        self._universe = [self.aapl]

    def add_to_window(self, price):
        if len(self.window) > 60:
            self.window.popleft()
        self.window.append(price)

    def get_sma(self):
        return sum(self.window) / len(self.window)

    def universe(self):
        return self._universe

    def cadence(self):
        return Cadence( 
            bar_size=timedelta(days=1), # default
            call_phase="on_bar_close", # default
            exec_lag_bars=1, # default
        )

    def on_data(self, data: Slice, portfolio: PortfolioView):
        if not data or not data.close(self.aapl):
            return None
        
        price = data.close(self.aapl)
        self.add_to_window(price)
        sma = self.get_sma()

        if price > sma:
            return {self.aapl: 1}
        else:
            return {self.aapl: 0}