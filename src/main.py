import asyncio
import logging
from datetime import datetime
from math import trunc

from hqg_algorithms import PortfolioView

from src.portfolio import Portfolio
from src.database import async_session
from src.db.models import (Portfolio as PortfolioDB, AllocationEvent as AllocationEventDB, ExecutionEvent as ExecutionEventDB, Action)
from sqlalchemy import select
from src.provider_instance.client import provider_client

logger = logging.getLogger(__name__)

def truncate(value: float) -> float:
    return trunc(float(value) * 1000) / 1000


def build_portfolio_view(equity, positions, market_data):
    for k in list(positions.keys()):
        positions[k] = float(positions[k])
    
    if equity <= 0:
        return PortfolioView(
            equity=float(equity),
            cash=0.0,
            positions=positions,
            weights={},
        )

    holdings_value = 0.0
    weights: dict[str, float] = {}

    for symbol, qty in positions.items():
        reference_price = None
        snapshot = market_data.get(symbol)
        if isinstance(snapshot, dict):
            for key in ("close", "price"):
                v = snapshot.get(key)
                if v is None:
                    continue
                try:
                    reference_price = float(v)
                    break
                except (TypeError, ValueError):
                    continue

        if reference_price is None:
            continue

        position_value = qty * reference_price
        holdings_value += position_value
        weights[symbol] = position_value / equity

    cash = max(float(equity) - holdings_value, 0.0)
    return PortfolioView(
        equity=float(equity),
        cash=cash,
        positions=positions,
        weights=weights,
    )

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
        last_portfolio_view: PortfolioView | None = None

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
                        equity = await provider_client.get_account_value()
                        pos_before = await provider_client.get_positions()
                        portfolio_view = build_portfolio_view(equity, pos_before, market_data)
                        target_weights = await portfolio.on_data(market_data, portfolio_view)

                        portfolio_value = portfolio_view.equity
                        logger.info(f"Current account value: ${portfolio_value:,.2f}")

                        # Execute rebalancing
                        logger.info(f"Rebalancing with target weights:{target_weights}")
                        await provider_client.rebalance(target_weights, portfolio_value, market_data)

                        pos_after = await provider_client.get_positions()
                        equity_after = await provider_client.get_account_value()
                        last_portfolio_view = build_portfolio_view(equity_after, pos_after, market_data)

                        logger.debug(
                            "Post-rebalance portfolio view: equity=%s cash=%s",
                            last_portfolio_view.equity,
                            last_portfolio_view.cash,
                        )

                        execution_events = []
                        symbols = set(pos_before.keys()) | set(pos_after.keys())

                        for symbol in symbols:
                            quantity_before = float(pos_before.get(symbol, 0.0))
                            quantity_after = float(pos_after.get(symbol, 0.0))
                            quantity_change = truncate(quantity_after - quantity_before)

                            if quantity_change == 0.0:
                                continue

                            action = Action.BUY if quantity_change > 0 else Action.SELL
                            execution_events.append(
                                ExecutionEventDB(
                                    portfolio_id=db_portfolio.portfolio_id,
                                    timestamp=datetime.utcnow(),
                                    action=action,
                                    symbol=symbol,
                                    quantity=abs(quantity_change),
                                )
                            )

                        if target_weights is not None:
                            allocation_event = AllocationEventDB(
                                portfolio_id=db_portfolio.portfolio_id,
                                timestamp=datetime.utcnow(),
                                allocations=target_weights,
                            )
                            session.add(allocation_event)

                        if execution_events:
                            session.add_all(execution_events)

                        if target_weights is not None or execution_events:
                            await session.commit()
                
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