import asyncio
import logging

from src.portfolio import Portfolio
from src.database import async_session
from src.db.models import Portfolio as PortfolioDB
from sqlalchemy import select
from src.provider_instance.client import provider_client

logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler("app.log", mode="a")]
    )


async def run():
    """Main engine loop"""
    try:
        portfolio = Portfolio(config_path="config/portfolio.yaml")
        universe = portfolio.get_tickers()
        
        logger.info(f"Trading universe: {universe}")
        logger.info("Starting trading engine")
        
        market_data = {}
        ticker_set = set(universe)

        async for snapshot in provider_client.stream_prices(universe):
            if snapshot is not None:
                market_data[snapshot['symbol']] = snapshot
            
            if set(market_data.keys()) >= ticker_set:
                await provider_client.pause_stream()

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
                        portfolio_value = await provider_client.get_account_value()
                        logger.info(f"Current account value: ${portfolio_value:,.2f}")
                        
                        # Execute rebalancing
                        logger.info(f"Rebalancing with target weights:{target_weights}")
                        await provider_client.rebalance(target_weights, portfolio_value, market_data)
                
                # Reset market data & wait 1 min before continuing
                # unless we want to use stream to calc ohlc... (currently only snapshot at 1 min intervals, not "1 min data")
                await asyncio.sleep(60)  
                market_data = {}
                
                # clear stale
                await provider_client.clear_queue()

                # resume provider
                await provider_client.resume_stream()
            
    except KeyboardInterrupt:
        logger.info("Stopped from keyboard interrupt")
        
    except Exception as e:
        logger.error(f"Error in trading engine: {e}", exc_info=True)
        raise

    finally:
        await provider_client.close()
        logger.info("Engine shutdown complete")


async def main():
    setup_logging()
    
    # create portfolio if it doesn't exist
    async with async_session() as session:
        result = await session.execute(select(PortfolioDB).where(PortfolioDB.portfolio_id == 1))
        db_portfolio = result.scalar_one_or_none()
        
        if db_portfolio is None:
            logger.info("Default portfolio not found, creating portfolio with id=1")
            new_portfolio = PortfolioDB(name="Husky Portfolio", is_active=True)
            session.add(new_portfolio)
            await session.commit()
            await session.refresh(new_portfolio)
            logger.info(f"Created portfolio: ID={new_portfolio.portfolio_id}, name='{new_portfolio.name}', is_active={new_portfolio.is_active}")
    
    try:
        await run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())