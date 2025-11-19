from enum import Enum
from typing import Optional, List, Dict
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class Timeframe(str, Enum):
    """current timeframe options for PM endpoints (subject to change, based on current frontend)"""
    THREE_MONTHS = "3M"
    SIX_MONTHS = "6M"
    YEAR_TO_DATE = "YTD"

class TradeResponse(BaseModel):
    """Expected data model for trading endpoints"""
    success: bool
    message: str

class EquityResponse(BaseModel):
    """Expected data model for equity curve endpoint"""
    # TODO: Define structure for time series data points (to make: database table)
    # Expected: List of (timestamp, equity_value) pairs or similar
    data: List[Tuple(str, float)]

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

class StockAllocationsResponse(BaseModel):
    """Expected data model for stock allocations"""
    # TODO: Define structure for allocations by stock/ticker
    # Expected: Dict of ticker -> allocation mapping
    allocations: Dict[str, float]

class EventsResponse(BaseModel):
    """Expected data model for portfolio events endpoint"""
    # TODO: Define structure for events list (db table)
    # what exactly do we want to return? (orders, trades, type, date?)
    events: List[Tuple(str, str, str, str)]



@router.post("/portfolio/{id}/stop", response_model=TradeResponse)
async def stop_trading(id: int):
    """cancel pending orders and prevent new ones"""
    # TODO: Implement stop trading logic
        # Cancel all pending orders for the portfolio
        # Set portfolio state to prevent new orders
        # Return success status
    return TradeResponse(
        success=True,
        message="Trading stopped successfully"
    )

@router.post("/portfolio/{id}/resume", response_model=TradeResponse)
async def resume_trading(id: int):
    """allow new orders to be placed"""
    # TODO: Implement resume trading logic
        # Set portfolio state to allow new orders
        # Return success status
    return TradeResponse(
        success=True,
        message="Trading resumed successfully"
    )

@router.post("/portfolio/{id}/liquidate", response_model=TradeResponse)
async def liquidate_all(id: int):
    """close all open positions"""
    # TODO: Implement liquidation logic
        # Close all open positions
        # Return success status
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

@router.get("/portfolio/{id}/allocations/stocks", response_model=StockAllocationsResponse)
async def get_stock_allocations(id: int):
    """get allocations grouped by stock/ticker"""
    # TODO: Implement stock allocations retrieval for portfolio {id}
        # Query current allocations (by ticker/stock symbol)
        # Return mapping of ticker -> allocation (weight or value)
    # Expected response: StockAllocationsResponse with ticker -> allocation mapping
    return StockAllocationsResponse()

@router.get("/portfolio/{id}/events", response_model=EventsResponse)
async def get_events(id: int, timeframe: Optional[Timeframe] = None):
    """get list of portfolio events (orders, trades, type, date)"""
    # TODO: Return list of portfolio events (orders placed, trades executed, type, date) (db table to store events)
    # Expected response: EventsResponse
    return EventsResponse()