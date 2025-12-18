import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Optional
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import async_session
from src.db.models import Portfolio, Instrument, PerformanceSnapshot, HoldingsSnapshot
from src.marketdata_provider.alpaca import AlpacaMarketData
from src.execution_provider.alpaca import AlpacaExecutor

logger = logging.getLogger(__name__)

class SnapshotJob:
    def __init__(self, config_path="config/engine.yaml"):
        self.config_path = config_path
        self.alpaca_config = {}
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.alpaca_data: Optional[AlpacaMarketData] = None
        self.alpaca_exec: Optional[AlpacaExecutor] = None
        self.load_config()
    
    def load_config(self):
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            config_file = Path(__file__).parent.parent / self.config_path
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        self.alpaca_config = config.get('alpaca_config', {})
        logger.info(f"Config loaded from {config_file}")
    
    def setup_alpaca(self):
        try:
            logger.info("Setting up Alpaca connection")
            
            self.alpaca_data = AlpacaMarketData(
                api_key=self.alpaca_config['api_key'],
                secret_key=self.alpaca_config['secret_key'],
                paper=True
            )

            self.alpaca_exec = AlpacaExecutor(
                api_key=self.alpaca_config['api_key'],
                secret_key=self.alpaca_config['secret_key'],
                paper=True
            )
            
            logger.info("Alpaca providers initialized")
            return self.alpaca_data, self.alpaca_exec
            
        except Exception as e:
            logger.error(f"Error setting up Alpaca: {e}")
            raise
    
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
    
    async def _get_price(self, symbol: str, alpaca_data: AlpacaMarketData):
        try:
            price_data = await alpaca_data.get_price(symbol)
            if price_data and 'price' in price_data:
                return float(price_data['price'])
            else:
                logger.warning(f"No price data returned for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}")
            return None
    
    async def portfolio_snapshot(self, portfolio_id, session: AsyncSession, alpaca_data: AlpacaMarketData, alpaca_exec: AlpacaExecutor):
        """
        Create portfolio snapshot records (PerformanceSnapshot and HoldingsSnapshot) for a single portfolio.
        """
        
        snapshot_date = date.today()
        
        try:
            equity = await alpaca_exec.get_account_value()
            positions = await alpaca_exec.get_positions()
            
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
                
                price = await self._get_price(symbol, alpaca_data)
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