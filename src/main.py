import asyncio
import os
import logging

from src.portfolio import Portfolio
from src.marketdata_provider.alpaca import AlpacaMarketData
from src.execution_provider.alpaca import AlpacaExecutor
from src.marketdata_provider.ibkr import IBKRMarketData
from src.execution_provider.ibkr import IBKRExecutor
from src.database import async_session
from src.db.models import Portfolio as PortfolioDB
from sqlalchemy import select


logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler("app.log", mode="a")]
    )


class Engine():
    def __init__(self):
        self.use_ib = None
        self.ib_config = {}
        self.alpaca_config = {}
        self.exec_provider = None
        self.data_provider = None
        self.load_config()

    def load_config(self):
        """Load configuration from env"""

        if os.environ.get("PROVIDER") == "ib":
            self.use_ib = True

        elif os.environ.get("PROVIDER") == "alpaca":
            self.use_ib = False

        else:
            raise ValueError("Invalid provider")
        
        if self.use_ib:
            self.ib_config = {
                'host': os.environ["IBKR_HOST"],
                'port': int(os.environ["IBKR_PORT"]),
                'client_id': int(os.environ["IBKR_CLIENT_ID"])
            }
            logger.info(f"Using IBKR. Config: {self.ib_config}")
       
        else:
            self.alpaca_config = {
                'api_key': os.environ["ALPACA_API_KEY"],
                'secret_key': os.environ["ALPACA_SECRET_KEY"],
                'paper': os.environ.get("ALPACA_PAPER", "true")
            }
            logger.info(f"Using Alpaca. Config: {self.alpaca_config}")
    
    async def setup_ib(self):
        """Setup IBKR connection"""
        try:
            from ib_async import IB
            
            ib = IB()
            await ib.connectAsync(
                host=self.ib_config['host'],
                port=self.ib_config['port'],
                clientId=self.ib_config['client_id']
            )
            
            if 'market_data_type' in self.ib_config:
                ib.reqMarketDataType(self.ib_config['market_data_type'])
            
            logger.info(f"Connected to IBKR at {self.ib_config['host']}:{self.ib_config['port']}")
            
            self.data_provider = IBKRMarketData(ib)
            self.exec_provider = IBKRExecutor(ib)
            
        except Exception as e:
            logger.error(f"Error connecting to IBKR: {e}")
            raise

    def setup_alpaca(self):
        """Setup Alpaca connection"""
        try:
            logger.info("Setting up Alpaca connection")
            
            self.data_provider = AlpacaMarketData(
                api_key=self.alpaca_config['api_key'],
                secret_key=self.alpaca_config['secret_key'],
                paper=True
            )

            self.exec_provider = AlpacaExecutor(
                api_key=self.alpaca_config['api_key'],
                secret_key=self.alpaca_config['secret_key'],
                paper=True
            )
            
            logger.info("Alpaca providers initialized")

        except Exception as e:
            logger.error(f"Error setting up Alpaca: {e}")
            raise
    
    def get_data_provider(self):
        return self.data_provider
    
    def get_exec_provider(self):
        return self.exec_provider

    async def run(self):
        """Main engine loop"""
        data_provider = None
        exec_provider = None
        
        try:
            portfolio = Portfolio(config_path="config/portfolio.yaml")
            universe = portfolio.get_tickers()
            
            logger.info(f"Trading universe: {universe}")

            # provider
            if self.use_ib:
                await self.setup_ib()
            else:
                self.setup_alpaca()
            
            logger.info("Starting trading engine")
            market_data = {}
            ticker_set = set(universe)

            async for snapshot in self.data_provider.stream_prices(universe):
                if snapshot is not None:
                        market_data[snapshot['symbol']] = snapshot
                
                if set(market_data.keys()) >= ticker_set:
                    await self.data_provider.pause_stream()

                    async with async_session() as session:
                        result = await session.execute(select(PortfolioDB).where(PortfolioDB.portfolio_id == 1)) # TODO: generalize to work with multiple portfolios
                        db_portfolio = result.scalar_one_or_none()
                        
                        if db_portfolio is None:
                            logger.warning("No portfolio found in database, skipping trading cycle")
                            continue
                        elif not db_portfolio.is_active:
                            logger.info("Portfolio is inactive, skipping trading cycle")
                            continue
                        else:
                            # Get target weights from portfolio
                            target_weights = await portfolio.on_data(market_data)

                            # Get current account value
                            portfolio_value = await self.exec_provider.get_account_value()
                            logger.info(f"Current account value: ${portfolio_value:,.2f}")
                            
                            # Execute rebalancing
                            logger.info(f"Rebalancing with target weights:{target_weights}")
                            await self.exec_provider.rebalance(target_weights, portfolio_value, market_data)
                    
                    # Reset market data & wait 1 min before continuing
                        # unless we want to use stream to calc ohlc... (currently only snapshot at 1 min intervals, not "1 min data")
                    await asyncio.sleep(60)  
                    market_data = {}
                    
                    # clear stale
                    await self.data_provider.clear_queue()

                    # resume provider
                    await self.data_provider.resume_stream()
                
        except KeyboardInterrupt:
            logger.info("Stopped from keyboard interrupt")
            
        except Exception as e:
            logger.error(f"Error in trading engine: {e}", exc_info=True)
            raise

        finally:
            # Cleanup connections
            if self.data_provider:
                await self.data_provider.cleanup()
            logger.info("Engine shutdown complete")


async def main():
    setup_logging()
    
    try:
        engine = Engine()
        await engine.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())