import asyncio
import logging
from datetime import datetime
from typing import AsyncIterator, Dict, List
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.data.historical import StockHistoricalDataClient
from .base import MarketData

logger = logging.getLogger(__name__)


class AlpacaMarketData(MarketData):
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self.data_stream = StockDataStream(
            api_key=api_key,
            secret_key=secret_key,
        )
        self.data_client = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key
        )
        self._tickers = {}
        self._running = False
        self._quote_queue = asyncio.Queue()
        self._stream_task = None
    
    
    async def get_price(self, symbol: str) -> Dict:
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            # TODO: fix get live data
            quote = await self.trading_client.get_stock_latest_quote(request)
            
            if not quote or symbol not in quote:
                logger.warning(f"No quote data for {symbol}")
                return None
            
            quote_data = quote[symbol]
            
            return {
                'symbol': symbol,
                'price': quote_data.ask_price,
                'bid': quote_data.bid_price,
                'ask': quote_data.ask_price,
                'timestamp': datetime.now(),
                'volume': quote_data.bid_size + quote_data.ask_size,
                'source': 'alpaca'
            }
            
        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}")
            return None


    async def stream_prices(self, symbols: List[str]) -> AsyncIterator[Dict]:
        """Stream prices - this is now an async generator"""
        try:
            self._running = True
            logger.info(f"Starting price stream for {symbols}")
            
            self.data_stream.subscribe_quotes(self._on_quote, *symbols)            
            self._stream_task = asyncio.create_task(self.data_stream._run_forever())    # note: error w run()
            
            # Yield quotes from the queue
            while self._running:
                try:
                    quote = await asyncio.wait_for(self._quote_queue.get(), timeout=1.0)
                    yield quote
                except asyncio.TimeoutError:
                    continue  # Check if still running
                except Exception as e:
                    logger.error(f"Error getting quote from queue: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Error streaming prices: {e}")
            raise
        finally:
            await self.cleanup()


    async def _on_quote(self, data):
        """Handle incoming quote data"""
        try:
            quote_dict = {
                'symbol': data.symbol,
                'price': data.ask_price,
                'bid': data.bid_price,
                'ask': data.ask_price,
                'bid_size': data.bid_size,
                'ask_size': data.ask_size,
                'timestamp': datetime.now(),
                'volume': data.bid_size + data.ask_size,
                'source': 'alpaca'
            }
            
            self._tickers[data.symbol] = quote_dict
            await self._quote_queue.put(quote_dict)
            
        except Exception as e:
            logger.error(f"Error processing quote: {e}")


    async def cleanup(self):
        """Clean up resources"""
        try:
            self._running = False
            
            # Cancel the stream task
            if self._stream_task and not self._stream_task.done():
                self._stream_task.cancel()
                try:
                    await self._stream_task
                except asyncio.CancelledError:
                    pass
            
            # Close the data stream
            if self.data_stream:
                try:
                    await self.data_stream.close()
                except Exception as e:
                    logger.warning(f"Error closing data stream: {e}")
                    
            logger.info("Alpaca data stream closed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")