import asyncio
from datetime import datetime
from ib_async import IB, Stock
from .base import MarketData

class IBKRMarketData(MarketData):
    def __init__(self, connection: IB):
        self.ib = connection
        self._tickers = {}
    
    
    async def get_price(self, symbol: str):
        contract = Stock(symbol, "SMART", "USD")
        contract = await self.ib.qualifyContractsAsync(contract)
        
        if not contract:
            raise ValueError(f"Could not qualify the contract for {symbol}")
        
        contract = contract[0]
        ticker = self.ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)
        
        await asyncio.sleep(0.5) # wait for ticker data
        
        return {
            'symbol': symbol,
            'price': ticker.last if ticker.last else ticker.close,
            'bid': ticker.bid,
            'ask': ticker.ask,
            'volume': ticker.volume,
            'timestamp': datetime.now()
        }


    async def stream_prices(self, symbols: list[str]):
        contracts = [Stock(sym, "SMART", "USD") for sym in symbols]
        qualified = await self.ib.qualifyContractsAsync(*contracts)
        
        if not qualified:
            raise ValueError("Could not qualify any contracts")
        
        for contract in qualified:
            ticker = self.ib.reqMktData(contract, "", snapshot=False, regulatorySnapshot=False)
            symbol = contract.symbol
            self._tickers[symbol] = ticker

        while True:
            await asyncio.sleep(60)  # wait 1 min per IBKR (to confirm)

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
                    'timestamp': datetime.now(),
                    'source': 'ibkr'
                }
    

    async def cleanup(self):
        for ticker in self._tickers.values():
            self.ib.cancelMktData(ticker.contract)
        self._tickers.clear()


    # TODO, if we want to keep the same structure in main
    async def pause_stream(self):
        pass
    async def resume_stream(self):
        pass
    async def clear_queue(self):
        pass