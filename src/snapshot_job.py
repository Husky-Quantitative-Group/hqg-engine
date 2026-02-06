import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Optional
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import async_session
from src.db.models import Portfolio, Instrument, PerformanceSnapshot, HoldingsSnapshot, StrategyWeightsSnapshot
from src.provider_instance.client import provider_client

logger = logging.getLogger(__name__)

class SnapshotJob:
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
    
    async def get_active_portfolios(self):
        async with async_session() as session:
            result = await session.execute(select(Portfolio).where(Portfolio.is_active == True))
            portfolios = result.scalars().all()
            return list(portfolios)
    
    async def _get_or_create_instrument(self, session: AsyncSession, ticker: str):
        result = await session.execute(select(Instrument).where(Instrument.ticker == ticker))
        instrument = result.scalar_one_or_none()
        
        if instrument is None:
            instrument = Instrument(ticker=ticker)
            session.add(instrument)
            await session.flush()
            logger.info(f"Created new instrument: {ticker}")
        
        return instrument
    
    async def _get_price(self, symbol: str):
        try:
            price = await provider_client.get_price(symbol)
            return price
        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}")
            return None
    
    async def portfolio_snapshot(self, portfolio_id, session: AsyncSession):
        """
        Create portfolio snapshot records (PerformanceSnapshot and HoldingsSnapshot) for a single portfolio.
        """
        
        snapshot_date = date.today()
        
        try:
            equity = await provider_client.get_account_value()
            positions = await provider_client.get_positions()
            
            performance_snapshot = PerformanceSnapshot(
                portfolio_id=portfolio_id,
                as_of=snapshot_date,
                equity=equity
            )
            session.add(performance_snapshot)
            
            holdings_snapshots = []
            for symbol, quantity in positions.items():
                if quantity == 0:
                    continue
                
                price = await self._get_price(symbol)
                if price is None:
                    logger.warning(f"Skipping {symbol} - failed to fetch price")
                    continue
                
                instrument = await self._get_or_create_instrument(session, symbol)
                market_value = float(quantity) * price
                
                holdings_snapshot = HoldingsSnapshot(
                    portfolio_id=portfolio_id,
                    as_of=snapshot_date,
                    instrument_id=instrument.instrument_id,
                    quantity=quantity,
                    price=price,
                    market_value=market_value
                )
                holdings_snapshots.append(holdings_snapshot)
            
            for snapshot in holdings_snapshots:
                session.add(snapshot)
            
            logger.info(
                f"Portfolio {portfolio_id}: Created snapshot with {len(holdings_snapshots)} holdings"
            )

        except Exception as e:
            logger.error(f"Portfolio {portfolio_id}: Error creating snapshot: {e}", exc_info=True)
            await session.rollback()
            raise
    
    async def strategy_weights_snapshot(self, portfolio_id, as_of_date, session: AsyncSession):
        # currently just using portfolio.yaml config. TODO: make this more flexible for the future.
        try:
            config_path = Path("src/config/portfolio.yaml")
            if not config_path.exists():
                config_path = Path(__file__).parent / "config" / "portfolio.yaml"
            
            if not config_path.exists():
                raise FileNotFoundError(f"Portfolio config not found: {config_path}")
            
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            strategies = config.get('strategies', [])
            if not strategies:
                logger.warning(f"No strategies found in config for portfolio {portfolio_id}")
                return
            
            for strategy_config in strategies:
                strategy_id = strategy_config.get('id')
                class_name = strategy_config.get('class_name')
                portfolio_weight = strategy_config.get('portfolio_weight')
                
                if strategy_id is None or class_name is None or portfolio_weight is None:
                    logger.warning(f"Invalid strategy config: {strategy_config}")
                    continue
                
                strategy_snapshot = StrategyWeightsSnapshot(
                    portfolio_id=portfolio_id,
                    as_of=as_of_date,
                    strategy_id=strategy_id,
                    strategy_name=class_name,
                    weight=portfolio_weight
                )
                session.add(strategy_snapshot)
            
            logger.info(
                f"Portfolio {portfolio_id}: Created {len(strategies)} strategy weight snapshots"
            )
            
        except Exception as e:
            logger.error(
                f"Portfolio {portfolio_id}: Error creating strategy weights snapshot: {e}",
            )
            raise
    
    async def run(self):
        logger.info("Starting snapshot job")

        snapshot_date = date.today()
        
        try:
            portfolios = await self.get_active_portfolios()
            
            if not portfolios:
                logger.info("No active portfolios found")
                return
            
            logger.info(f"Found {len(portfolios)} active portfolio(s)")
            
            for portfolio in portfolios:
                try:
                    logger.info(f"Processing portfolio {portfolio.portfolio_id}: {portfolio.name}")
                    
                    async with async_session() as portfolio_session:
                        await self.portfolio_snapshot(
                            portfolio.portfolio_id,
                            portfolio_session
                        )
                        await portfolio_session.commit()
                    
                    async with async_session() as weights_session:
                        await self.strategy_weights_snapshot(
                            portfolio.portfolio_id,
                            snapshot_date,
                            weights_session
                        )
                        await weights_session.commit()
                    
                    logger.info(f"Successfully created snapshots for portfolio {portfolio.portfolio_id}")

                except Exception as e:
                    logger.error(
                        f"Failed to create snapshot for portfolio {portfolio.portfolio_id}: {e}",
                        exc_info=True
                    )
                    continue
            
            logger.info("Snapshot job completed")
        
        except Exception as e:
            logger.error(f"Error in snapshot job: {e}")
            raise
    
    def schedule(self):
        if self.scheduler is not None:
            logger.warning("Scheduler already initialized")
            return
        
        self.scheduler = AsyncIOScheduler()
        
        # setup timezone for EDT
        est_tz = ZoneInfo('US/Eastern')

        self.scheduler.add_job(
            self.run,
            trigger=CronTrigger(hour=16, minute=0, timezone=est_tz),
            id='daily_snapshot_job',
            name='Daily Portfolio Snapshot',
            replace_existing=True
        )
        
        self.scheduler.start()
        logger.info("Snapshot job scheduled for daily execution at 4 PM EST")
    
    def shutdown(self):
        if self.scheduler:
            self.scheduler.shutdown()
            logger.info("Scheduler shut down")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    
    job = SnapshotJob()
    
    try:
        job.schedule()
        
        logger.info("Snapshot job scheduler started.")
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        job.shutdown()
        logger.info("Snapshot job stopped")


if __name__ == "__main__":
    asyncio.run(main())