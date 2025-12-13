from enum import Enum
from typing import Optional, List, Dict
import statistics
from fastapi import APIRouter, HTTPException, Depends
from datetime import date, datetime, timedelta
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_session

from src.db.models import (
    Portfolio,
    Instrument,
    PerformanceSnapshot,
    HoldingsSnapshot,
    StrategyWeightsSnapshot,
    ExecutionEvent,
    AllocationEvent,
    Action
)

router = APIRouter()

def timeframe_to_date_range(timeframe: Optional[Timeframe]):
    if timeframe is None:
        return None # update to what we want our default to be
    
    today = date.today()
    
    if timeframe == Timeframe.THREE_MONTHS:
        return today - timedelta(days=90)
    elif timeframe == Timeframe.SIX_MONTHS:
        return today - timedelta(days=180)
    elif timeframe == Timeframe.YEAR_TO_DATE:
        return date(today.year, 1, 1)

async def get_portfolio(portfolio_id: int, session: AsyncSession):
    result = await session.execute(select(Portfolio).where(Portfolio.portfolio_id == portfolio_id))
    portfolio = result.scalar_one_or_none()
    
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {portfolio_id} not found")
    
    return portfolio

class Timeframe(str, Enum):
    """current timeframe options for PM endpoints (subject to change, based on current frontend)"""
    THREE_MONTHS = "3M"
    SIX_MONTHS = "6M"
    YEAR_TO_DATE = "YTD"

class TradeResponse(BaseModel):
    """Expected data model for trading endpoints"""
    success: bool
    message: str

class EquityPoint(BaseModel):
    """Expected data model for equity curve data points"""
    timestamp: str
    equity_value: float

class EquityResponse(BaseModel):
    """Expected data model for equity curve endpoint"""
    data: List[EquityPoint]

class SnapshotResponse(BaseModel):
    """Expected data model for portfolio snapshot endpoint"""
    # TODO: Define structure for main metrics
    # Expected: equity, capital, net_profit, return_pct
    equity: float
    capital: float
    net_profit: float
    return_pct: float

class MetricsResponse(BaseModel):
    """Expected data model for portfolio metrics"""
    # TODO: Define structure for performance metrics 
    # Expected: Dict of metric_name -> value (Sharpe, Sortino, CAGR, max_drawdown, alpha, beta, std)
    metrics: Dict[str, float]

class StrategyAllocationsResponse(BaseModel):
    """Expected data model for strategy allocations"""
    # TODO: Define structure for allocations by strategy
    # Expected: Dict of strategy_id -> allocation mapping
    allocations: Dict[str, float]

class AssetAllocationsResponse(BaseModel):
    """Expected data model for asset allocations"""
    # TODO: Define structure for allocations by asset symbol
    # Expected: Dict of symbol -> allocation mapping
    allocations: Dict[str, float]

class ExecutionEvent(BaseModel):
    """Data model for individual execution events (buy/sell orders)"""
    action: str
    symbol: str
    quantity: float
    timestamp: str

class ExecutionEventsResponse(BaseModel):
    """Expected data model for execution event endpoint"""
    events: List[ExecutionEvent]

class AllocationEvent(BaseModel):
    """Data model for portfolio allocation/rebalancing events"""
    timestamp: str
    allocations: AssetAllocationsResponse

class AllocationEventsResponse(BaseModel):
    """Expected data model for allocation events endpoint"""
    events: List[AllocationEvent]



@router.post("/portfolio/{id}/stop", response_model=TradeResponse)
async def stop_trading(id: int, session: AsyncSession = Depends(get_session)):
    portfolio = await get_portfolio(id, session)
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")

    # ASSUMPTION: some "stop_trading(portfolio_id)" will be defined in src/portfolio.py (add import here after)
    # stop_trading(id)
    portfolio.is_active = False
    await session.commit()
    
    return TradeResponse(
        success=True,
        message="Trading stopped successfully"
    )

@router.post("/portfolio/{id}/resume", response_model=TradeResponse)
async def resume_trading(id: int, session: AsyncSession = Depends(get_session)):
    portfolio = await get_portfolio(id, session)
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")

    # ASSUMPTION: same as for stop_trading endpt.
    # resume_trading(id)
    portfolio.is_active = True
    await session.commit()
    
    return TradeResponse(
        success=True,
        message="Trading resumed successfully"
    )

@router.post("/portfolio/{id}/liquidate", response_model=TradeResponse)
async def liquidate_all(id: int, session: AsyncSession = Depends(get_session)):
    portfolio = await get_portfolio(id, session)
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")

    # ASSUMPTION: same as for stop and resume_trading endpts.
    # liquidate(id)
    await session.commit()
    
    return TradeResponse(
        success=True,
        message="Liquidation initiated successfully"
    )



@router.get("/portfolio/{id}/equity", response_model=EquityResponse)
async def get_equity(id: int, timeframe: Optional[Timeframe] = None, session: AsyncSession = Depends(get_session)):
    await get_portfolio(id, session)
    query = select(PerformanceSnapshot).where(PerformanceSnapshot.portfolio_id == id)

    start_date = timeframe_to_date_range(timeframe)
    query = query.where(PerformanceSnapshot.as_of >= start_date)
    query = query.order_by(PerformanceSnapshot.as_of)

    result = await session.execute(query)
    snapshots = result.scalars().all()

    if snapshots is None:
        raise HTTPException(status_code=404, detail=f"No equity snapshots found for portfolio {id}")

    equity_points = [
        EquityPoint(
            timestamp=snapshot.as_of.isoformat(),
            equity_value=float(snapshot.equity)
        ) for snapshot in snapshots
    ]
    
    return EquityResponse(data=equity_points)

@router.get("/portfolio/{id}/snapshot", response_model=SnapshotResponse)
async def get_snapshot(id: int, timeframe: Optional[Timeframe] = None, session: AsyncSession = Depends(get_session)):
    """get main portfolio metrics (equity, capital, net profit, return %)"""
    await get_portfolio(id, session)
    query = select(PerformanceSnapshot).where(PerformanceSnapshot.portfolio_id == id)

    start_date = timeframe_to_date_range(timeframe)
    query = query.where(PerformanceSnapshot.as_of >= start_date)
    query = query.order_by(PerformanceSnapshot.as_of.desc())
    
    result = await session.execute(query)
    performance_snapshot = result.scalar_one_or_none()

    if performance_snapshot is None:
        raise HTTPException(status_code=404, detail=f"No snapshot found for portfolio {id}")
    
    snapshot_date = performance_snapshot.as_of
    equity = float(performance_snapshot.equity)
    
    # get holdings for the snapshot date (to calculate capital)
    holdings_query = select(HoldingsSnapshot).where(
        HoldingsSnapshot.portfolio_id == id,
        HoldingsSnapshot.as_of == snapshot_date
    )

    holdings_result = await session.execute(holdings_query)
    holdings = holdings_result.scalars().all()
    
    capital = sum(float(holding.market_value) for holding in holdings)
    
    # get baseline snapshot (for profit/return calculations)
    baseline_query = select(PerformanceSnapshot).where(PerformanceSnapshot.portfolio_id == id)
    
    baseline_query = baseline_query.where(PerformanceSnapshot.as_of >= start_date)
    baseline_query = baseline_query.order_by(PerformanceSnapshot.as_of)

    baseline_result = await session.execute(baseline_query)
    baseline_snapshot = baseline_result.scalar_one_or_none()
    initial_capital = float(baseline_snapshot.equity)
    
    # calculate performance metrics
    net_profit = equity - initial_capital
    return_pct = net_profit / initial_capital if initial_capital > 0 else 0.0

    return SnapshotResponse(
        equity=equity,
        capital=capital,
        net_profit=net_profit,
        return_pct=return_pct
    )

@router.get("/portfolio/{id}/metrics", response_model=MetricsResponse)
async def get_metrics(id: int, timeframe: Optional[Timeframe] = None, session: AsyncSession = Depends(get_session)):
    """get other performance metrics (sharpe, sortino, CAGR, max drawdown, alpha, beta, std)"""
    await get_portfolio(id, session)
    
    # get performance snapshots (within timeframe)
    query = select(PerformanceSnapshot).where(PerformanceSnapshot.portfolio_id == id)
    start_date = timeframe_to_date_range(timeframe)
    query = query.where(PerformanceSnapshot.as_of >= start_date)
    query = query.order_by(PerformanceSnapshot.as_of)
    
    result = await session.execute(query)
    snapshots = result.scalars().all()
    
    if len(snapshots) < 2: # need at least 2 data points
        return MetricsResponse(metrics={"sharpe": 0.0, "sortino": 0.0, "cagr": 0.0, "max_drawdown": 0.0, "alpha": 0.0, "beta": 0.0, "std": 0.0})
    
    # equity values and dates
    equity_values = [float(snapshot.equity) for snapshot in snapshots]
    dates = [snapshot.as_of for snapshot in snapshots]
    
    # calculate returns
    returns = []
    for i in range(1, len(equity_values)): # possible check for divide by 0?
        ret = (equity_values[i] - equity_values[i-1]) / equity_values[i-1]
        returns.append(ret)
    
    # calculate CAGR
    initial_value = equity_values[0]
    final_value = equity_values[-1]
    days_diff = (dates[-1] - dates[0]).days
    years = days_diff / 365 if days_diff > 0 else 1
    cagr = ((final_value / initial_value) ** (1 / years) - 1) if initial_value > 0 else 0
    
    # calculate drawdown
    peak = initial_value
    max_drawdown = 0.0
    for value in equity_values:
        if value > peak:
            peak = value
        drawdown = (peak - value) / peak if peak > 0 else 0.0
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    
    # calculate std
    std = statistics.stdev(returns)
    
    # calculate sharpe ratio (temporary risk-free = 0; should we store T-Bill rate in db as well?)
    mean_return = statistics.mean(returns)
    sharpe = (mean_return / std)
    
    # calculate sortino
    downside_returns = [r for r in returns if r < 0]
    downside_std = statistics.stdev(downside_returns)
    sortino = (mean_return / downside_std)
    
    # alpha and beta, TBD (0.0 for now)
    alpha = 0.0 # similar to sharpe, should we store a benchmark rate in db?
    beta = 0.0 # ... also
    
    return MetricsResponse(metrics={
        "sharpe": sharpe,
        "sortino": sortino,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "alpha": alpha,
        "beta": beta,
        "std": std
    })

@router.get("/portfolio/{id}/allocations/strategies", response_model=StrategyAllocationsResponse)
async def get_strategy_allocations(id: int, session: AsyncSession = Depends(get_session)):
    """get allocations grouped by strategy name"""
    await get_portfolio(id, session)
    
    # get most recent snapshot date
    query = select(StrategyWeightsSnapshot).where(StrategyWeightsSnapshot.portfolio_id == id)
    query = query.order_by(StrategyWeightsSnapshot.as_of.desc())
    result = await session.execute(query)
    snapshots = result.scalars().all()
    
    if not snapshots:
        return StrategyAllocationsResponse(allocations={})
    
    # get most recent & filter to only those snapshots
    most_recent = snapshots[0].as_of
    weights = [snapshot for snapshot in snapshots if snapshot.as_of == most_recent]
    
    allocations = {snapshot.strategy_name : float(snapshot.weight) for snapshot in weights}
    return StrategyAllocationsResponse(allocations=allocations)

@router.get("/portfolio/{id}/allocations/assets", response_model=AssetAllocationsResponse)
async def get_asset_allocations(id: int):
    """get allocations grouped by symbols"""
    # TODO: Implement asset allocations retrieval for portfolio {id}
        # Query current allocations (by symbol)
        # Return mapping of symbols -> allocation (weight or value)
    # Expected response: AssetAllocationsResponse with symbols -> allocation mapping
    return AssetAllocationsResponse()

@router.get("/portfolio/{id}/events/executions", response_model=ExecutionEventsResponse)
async def get_execution_events(id: int, timeframe: Optional[Timeframe] = None):
    """get list of execution events"""
    # TODO: Return list of execution events (orders placed, trades executed)
    return ExecutionEventsResponse(events=[])

@router.get("/portfolio/{id}/events/allocations", response_model=AllocationEventsResponse)
async def get_allocation_events(id: int, timeframe: Optional[Timeframe] = None):
    """get list of rebalance events"""
    # TODO: Return list of rebalance events
    return AllocationEventsResponse(events=[])