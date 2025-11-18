from enum import Enum
from typing import Optional
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


class MetricResponse(BaseModel):
    """Expected data model for PM metrics"""
    value: float
    timeframe: Optional[str] = None


class OrderResponse(BaseModel): # maybe this is not necessary? not too sure
    """Expected data model for orders endpoint"""
    count: int
    timeframe: Optional[str] = None


@router.post("/trade/stop", response_model=TradeResponse)
async def stop_trading():
    """cancel pending orders and prevent new ones"""
    # TODO: Implement stop trading logic
    return TradeResponse(
        success=True,
        message="Trading stopped successfully"
    )


@router.post("/trade/resume", response_model=TradeResponse)
async def resume_trading():
    """allow new orders to be placed"""
    # TODO: Implement resume trading logic
    return TradeResponse(
        success=True,
        message="Trading resumed successfully"
    )


@router.post("/trade/liquidate", response_model=TradeResponse)
async def liquidate_all():
    """close all open positions"""
    # TODO: Implement liquidation logic
    return TradeResponse(
        success=True,
        message="Liquidation initiated successfully"
    )


@router.get("/portfolio/capital", response_model=MetricResponse)
async def get_invested_capital():
    """get total invested capital"""
    # TODO: Implement invested capital calculation
    return MetricResponse(
        value=0.0,
        unit="USD"
    )


@router.get("/portfolio/equity", response_model=MetricResponse)
async def get_current_equity(timeframe: Optional[Timeframe] = None):
    """get current equity value"""
    # TODO: Implement current equity calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/net-profit", response_model=MetricResponse)
async def get_net_profit(timeframe: Optional[Timeframe] = None):
    """calculate and return net profit after fees and carry"""
    # TODO: Implement net profit calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/return", response_model=MetricResponse)
async def get_return_pct(timeframe: Optional[Timeframe] = None):
    """get return percentage"""
    # TODO: Implement return percentage calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/sharpe", response_model=MetricResponse)
async def get_sharpe_ratio(timeframe: Optional[Timeframe] = None):
    """calculate and return Sharpe (within timeframe)"""
    # TODO: Implement Sharpe ratio calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/sortino", response_model=MetricResponse)
async def get_sortino_ratio(timeframe: Optional[Timeframe] = None):
    """calculate and return Sortino (within timeframe)"""
    # TODO: Implement Sortino ratio calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/cagr", response_model=MetricResponse)
async def get_cagr(timeframe: Optional[Timeframe] = None):
    """calculate and return CAGR (within timeframe)"""
    # TODO: Implement CAGR calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/max-drawdown", response_model=MetricResponse)
async def get_max_drawdown(timeframe: Optional[Timeframe] = None):
    """calculate and return max drawdown (within timeframe)"""
    # TODO: Implement drawdown calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/alpha", response_model=MetricResponse)
async def get_alpha(timeframe: Optional[Timeframe] = None):
    """calculate and return alpha (vs benchmark)"""
    # TODO: Implement alpha calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/beta", response_model=MetricResponse)
async def get_beta(timeframe: Optional[Timeframe] = None):
    """calculate and return beta (vs benchmark (SPX?))"""
    # TODO: Implement beta calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/std", response_model=MetricResponse)
async def get_annual_std(timeframe: Optional[Timeframe] = None):
    """calculate and return std (within timeframe)"""
    # TODO: Implement standard deviation calculation (w/ timeframe)
    return MetricResponse(
        value=0.0,
        timeframe=timeframe.value if timeframe else None
    )


@router.get("/portfolio/order-count", response_model=OrderResponse)
async def get_orders_count(timeframe: Optional[Timeframe] = None):
    """return count of orders (within timeframe)"""
    # TODO: Implement order count calculation (w/ timeframe)
    return OrderResponse(
        count=0,
        timeframe=timeframe.value if timeframe else None
    )