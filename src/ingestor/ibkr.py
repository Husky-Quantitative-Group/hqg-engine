import asyncio
from datetime import datetime
from ib_async import IB, Stock
from .base import MarketDataProvider
import math
from enum import Enum

class MarketDataType(Enum):
    REALTIME = 1
    FROZEN = 2
    DELAYED = 3
    DELAYED_FROZEN = 4

class IBData(MarketDataProvider):
    def __init__(self, connection: IB):
        self.ib = connection
        self.ib.reqMarketDataType(MarketDataType.DELAYED.value)
        self._tickers = {}
    
    
    async def get_price(self, symbol: str):
        contract = Stock(symbol, "SMART", "USD")
        contract = await self.ib.qualifyContractsAsync(contract)
        
        if not contract:
            raise ValueError(f"Could not qualify the contract for {symbol}")
        contract = contract[0]
        ticker = self.ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)
        
        # wait 3 sec
        for _ in range(15):
            await asyncio.sleep(0.2)
            if (ticker.last and not math.isnan(ticker.last)) or (ticker.close and not math.isnan(ticker.close)):
                break

        return {
            'symbol': symbol,
            'price': ticker.last if ticker.last else ticker.close,
            'bid': ticker.bid,
            'ask': ticker.ask,
            'volume': ticker.volume,
            'timestamp': datetime.now()
        }


    async def stream_prices(self, symbols: list[str], interval:float=60.0):
        contracts = [Stock(sym, "SMART", "USD") for sym in symbols]
        qualified = await self.ib.qualifyContractsAsync(*contracts)
        
        if not qualified:
            raise ValueError("Could not qualify any contracts")
        
        for contract in qualified:
            ticker = self.ib.reqMktData(contract, "", snapshot=False, regulatorySnapshot=False)
            symbol = contract.symbol
            self._tickers[symbol] = ticker

        while True:
            await asyncio.sleep(interval)

            for symbol, ticker in self._tickers.items():
                price = ticker.last if ticker.last else ticker.close
                
                yield {
                    'symbol': symbol,
                    'price': price,
                    'bid': ticker.bid,
                    'ask': ticker.ask,
                    'volume': ticker.volume,
                    'open': ticker.open,
                    'high': ticker.high,
                    'low': ticker.low,
                    'close': ticker.close,
                    'timestamp': datetime.now()
                }
    

    async def cleanup(self):
        for ticker in self._tickers.values():
            self.ib.cancelMktData(ticker.contract)
        self._tickers.clear()

