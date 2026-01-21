import json
import logging
from typing import List, Dict, Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.provider_instance import engine_instance

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/market_data/price/{symbol}")
async def get_price(symbol: str):
    try:
        price_data = await engine_instance.data_provider.get_price(symbol)
        if price_data and 'price' in price_data:
            return float(price_data['price'])
        else:
            logger.warning(f"No price data returned for {symbol}", exc_info=True)
            return None
    except Exception as e:
        logger.error(f"Error fetching price for {symbol}: {e}", exc_info=True)
        return None


@router.get("/market_data/stream")
async def stream_prices(symbols: str = Query()):
    # streaming prices via SSE (server-sent events)
    try:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
        if not symbol_list:
            raise HTTPException(status_code=400, detail="No symbols provided")
        
        async def event_generator():
            try:
                async for price_data in engine_instance.data_provider.stream_prices(symbol_list):
                    if price_data:
                        yield f"data: {json.dumps(price_data)}\n\n"
            except Exception as e:
                logger.error(f"Error in price stream: {e}", exc_info=True)
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
        
        return StreamingResponse(event_generator(), media_type="text/event-stream")
    
    except Exception as e:
        logger.error(f"Error starting price stream: {e}", exc_info=True)
        return None


@router.post("/market_data/pause")
async def pause_stream():
    try:
        await engine_instance.data_provider.pause_stream()
        return {"status": "paused"}
    except Exception as e:
        logger.error(f"Error pausing stream: {e}", exc_info=True)
        return None


@router.post("/market_data/resume")
async def resume_stream():
    try:
        await engine_instance.data_provider.resume_stream()
        return {"status": "resumed"}
    
    except Exception as e:
        logger.error(f"Error resuming stream: {e}", exc_info=True)
        return None


@router.post("/market_data/clear_queue")
async def clear_queue():
    try:
        await engine_instance.data_provider.clear_queue()
        return {"status": "cleared"}
    
    except Exception as e:
        logger.error(f"Error clearing queue: {e}", exc_info=True)
        return None


@router.get("/portfolio/account_value")
async def get_account_value():
    try:
        equity = await engine_instance.exec_provider.get_account_value()
        return equity
    
    except Exception as e:
        logger.error(f"Error getting account value: {e}", exc_info=True)
        return None


@router.get("/portfolio/positions")
async def get_positions():
    try:
        positions = await engine_instance.exec_provider.get_positions()
        return positions

    except Exception as e:
        logger.error(f"Error getting positions: {e}", exc_info=True)
        return None


@router.post("/execution/order")
async def place_order(symbol, quantity, side):
    try:
        order_id = await engine_instance.exec_provider.place_order(
            symbol,
            quantity,
            side
        )
        
        if order_id is None:
            logger.warning("Failed to place order", exc_info=True)
            return None
        
        return {"order_id": order_id, "symbol": symbol, "quantity": quantity, "side": side}
    
    except Exception as e:
        logger.error(f"Error placing order: {e}", exc_info=True)
        return None


@router.post("/execution/rebalance")
async def rebalance(target_weights: Dict[str, float], portfolio_value: float, market_data: Dict[str, float]):
    try:
        await engine_instance.exec_provider.rebalance(
            target_weights,
            portfolio_value,
            market_data
        )
        
        return {"status": "rebalanced", "target_weights": target_weights}
        
    except Exception as e:
        logger.error(f"Error rebalancing: {e}", exc_info=True)
        return None


@router.post("/execution/liquidate")
async def liquidate():
    try:
        logger.info("Liquidating portfolio")
        await engine_instance.exec_provider.liquidate_portfolio()

        logger.info("Portfolio liquidated")
        return {"status": "liquidated"}

    except Exception as e:
        logger.error(f"Error liquidating portfolio: {e}", exc_info=True)
        return None


@router.get("/health")
async def health():
    return {
        "status": "healthy",
        "data_provider_initialized": engine_instance.data_provider is not None,
        "exec_provider_initialized": engine_instance.exec_provider is not None
    }


@router.get("/status")
async def status():
    return {
        "provider": "ib" if engine_instance.use_ib else "alpaca",
        "data_provider_initialized": engine_instance.data_provider is not None,
        "exec_provider_initialized": engine_instance.exec_provider is not None
    }