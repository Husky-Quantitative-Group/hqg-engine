import asyncio 
import logging
import os
from datetime import datetime
from typing import AsyncIterator, Dict, List
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data import StockLatestTradeRequest

from .base import MarketData

logger = logging.getLogger(__name__)


class AlpacaMarketData(MarketData):
    def __init__(self, api_key: str, secret_key: str, paper: bool = True, poll_interval: float = 30.0):
        https_proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
        http_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
        
        self.data_client = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key
        )
        self._tickers = {}
        self._running = False
        self._quote_queue = asyncio.Queue()
        self._poll_task = None
        self._ispaused = False
        self._poll_interval = poll_interval  # seconds between REST API polls
    
    
    async def get_price(self, symbol: str) -> Dict:
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            quote = self.data_client.get_stock_latest_quote(request)

            if not quote or symbol not in quote:
                logger.warning(f"No quote data for {symbol}")
                return None
            
            quote_data = quote[symbol]
            
            return {
                'symbol': symbol,
                'price': quote_data.ask_price,
                'bid': quote_data.bid_price,
                'ask': quote_data.ask_price,
                'timestamp': datetime.now().isoformat(),
                'source': 'alpaca'
            }
            
        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}")
            return None


    async def stream_prices(self, symbols: List[str]) -> AsyncIterator[Dict]:
        """Async generator"""
        try:
            self._running = True
            self._tickers = {symbol: None for symbol in symbols}
            logger.info(f"Starting price stream for {symbols}")
            
            self._poll_task = asyncio.create_task(self._poll_quotes())
            
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

    async def pause_stream(self):
        self._ispaused = True
    
    async def resume_stream(self):
        self._ispaused = False
    
    async def clear_queue(self):
        self._quote_queue = asyncio.Queue()

    async def _poll_quotes(self):
        while self._running:
            try:
                if self._ispaused:
                    await asyncio.sleep(self._poll_interval)
                    continue
                
                symbols = list(self._tickers.keys())
                if not symbols:
                    await asyncio.sleep(self._poll_interval)
                    continue
                
                request = StockLatestQuoteRequest(symbol_or_symbols=symbols)
                quotes = self.data_client.get_stock_latest_quote(request)
                
                for symbol, quote_data in quotes.items():
                    await self._process_quote(symbol, quote_data)
                
                await asyncio.sleep(self._poll_interval)
                
            except Exception as e:
                logger.error(f"Error polling quotes: {e}")
                await asyncio.sleep(self._poll_interval)  # still polling (even on error)
    
    async def _process_quote(self, symbol: str, quote_data):
        """Process incoming quote data"""
        try:
            quote_dict = {
                'symbol': symbol,
                'price': quote_data.ask_price,
                'close': quote_data.ask_price,  # no "close" bc snapshot, this is suboptimal. may need to change hqg slice
                'bid': quote_data.bid_price,
                'ask': quote_data.ask_price,
                'bid_size': quote_data.bid_size,
                'ask_size': quote_data.ask_size,
                'timestamp': datetime.now().isoformat(),
                'volume': quote_data.bid_size + quote_data.ask_size,
                'source': 'alpaca'
            }

            last = self._tickers.get(symbol)
            if last:
                # Ignore exact duplicates
                if (last['bid'] == quote_dict['bid'] and
                    last['ask'] == quote_dict['ask'] and
                    last['bid_size'] == quote_dict['bid_size'] and
                    last['ask_size'] == quote_dict['ask_size']):
                    return
            
            self._tickers[symbol] = quote_dict
            await self._quote_queue.put(quote_dict)
            
        except Exception as e:
            logger.error(f"Error processing quote for {symbol}: {e}")


    async def cleanup(self):
        """Clean up resources"""
        try:
            self._running = False
            
            # Cancel the poll task
            if self._poll_task and not self._poll_task.done():
                self._poll_task.cancel()
                try:
                    await self._poll_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("Alpaca REST API polling stopped")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")