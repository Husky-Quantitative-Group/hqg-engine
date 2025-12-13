from enum import Enum
from typing import Optional, List, Dict
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
    return None

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
    result = await session.execute(
        select(Portfolio).where(Portfolio.portfolio_id == id)
    )
    
    portfolio = result.scalar_one_or_none()

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
    result = await session.execute(
        select(Portfolio).where(Portfolio.portfolio_id == id)
    )
    
    portfolio = result.scalar_one_or_none()

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
    result = await session.execute(
        select(Portfolio).where(Portfolio.portfolio_id == id)
    )
    
    portfolio = result.scalar_one_or_none()

    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")

    # ASSUMPTION: same as for stop and resume_trading endpts.
    # liquidate(id)
    
    return TradeResponse(
        success=True,
        message="Liquidation initiated successfully"
    )



@router.get("/portfolio/{id}/equity", response_model=EquityResponse)
async def get_equity(id: int, timeframe: Optional[Timeframe] = None):
    """get equity curve time series data"""
    # TODO: Return equity time series data (back to a given timeframe) (db table to maintain equity value)
    # Expected response: EquityResponse
    return EquityResponse(data=[])

@router.get("/portfolio/{id}/snapshot", response_model=SnapshotResponse)
async def get_snapshot(id: int, timeframe: Optional[Timeframe] = None):
    """get main portfolio metrics (equity, capital, net profit, return %)"""
    # TODO: Return most recent snapshot for a timeframe (db table maintains equity value, total capital, net profit, return percentage)
    # Expected response: SnapshotResponse with equity, capital, net_profit, return_pct
    return SnapshotResponse()

@router.get("/portfolio/{id}/metrics", response_model=MetricsResponse)
async def get_metrics(id: int, timeframe: Optional[Timeframe] = None):
    """get all other performance metrics (Sharpe, Sortino, CAGR, max drawdown, alpha, beta, std)"""
    # TODO: Return metrics for a portfolio (Sharpe ratio, Sortino ratio, CAGR, max drawdown, alpha, beta, std)
    # Filter by timeframe if provided
    # Expected response: MetricsResponse with dictionary of metric_name -> value
    return MetricsResponse()

@router.get("/portfolio/{id}/allocations/strategies", response_model=StrategyAllocationsResponse)
async def get_strategy_allocations(id: int):
    """get allocations grouped by strategy ID"""
    # TODO: Implement strategy allocations retrieval for portfolio {id}
        # Query current allocations (by strategy_id)
        # Return mapping of strategy_id -> allocation (weight or value)
    # Expected response: StrategyAllocationsResponse with strategy_id -> allocation mapping
    return StrategyAllocationsResponse()

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