from datetime import timedelta
from hqg_algorithms import Strategy, Cadence, Slice, PortfolioView

class ClassicFinance_SPY_IEF(Strategy):
    def __init__(self):
        self.spy = "SPY"
        self.ief = "IEF"
        self._universe = [self.spy, self.ief]
    
    def universe(self):
        return self._universe

    def cadence(self):
        return Cadence( 
            bar_size=timedelta(days=1), # default
            call_phase="on_bar_close", # default
            exec_lag_bars=1, # default
        )

    def on_data(self, data: Slice, portfolio: PortfolioView):
        if data.close(self.spy) and data.close(self.ief):
            return {self.spy: 0.6, self.ief: 0.4}
        return None