import asyncio
import logging
from typing import Dict, List
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from .base import Executor

logger = logging.getLogger(__name__)


class AlpacaExecutor(Executor):
    
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self.trading_client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper
        )
        self.order_history = []

    
    def get_order_history(self) -> List:
        return self.order_history


    async def get_account_value(self) -> float:
        try:
            account = self.trading_client.get_account()
            portfolio_value = float(account.equity)
            logger.info(f"Account value: ${portfolio_value}")
            return portfolio_value
        except Exception as e:
            logger.error(f"Error fetching account value: {e}")
            raise

 
    async def place_order(self, symbol: str, quantity: int, side: str) -> str:
        """
        Place a market order
        Returns: Order ID
        """
        try:
            if quantity <= 0:
                logger.warning(f"Invalid quantity {quantity} for {symbol}")
                return None
            
            order_side = OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL
            
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=order_side,
                time_in_force=TimeInForce.DAY
            )
            
            order = self.trading_client.submit_order(order_request)
            self.order_history.append(order)
            
            logger.info(f"Order placed: {side} {quantity} {symbol}, Order ID: {order.id}")
            return order.id
            
        except Exception as e:
            logger.error(f"Error placing order for {symbol}: {e}")
            raise

    
    async def get_positions(self) -> Dict[str, float]:
        try:
            positions = self.trading_client.get_all_positions()
            
            position_dict = {}
            for position in positions:
                symbol = position.symbol
                quantity = float(position.qty)
                position_dict[symbol] = quantity
            
            logger.info(f"Current positions: {position_dict}")
            return position_dict
            
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            raise

    
    async def get_order_status(self, order_id: str) -> str:
        try:
            order = self.trading_client.get_order_by_id(order_id)
            status = order.status
            logger.info(f"Order {order_id} status: {status}")
            return status
            
        except Exception as e:
            logger.error(f"Error fetching order status for {order_id}: {e}")
            raise


    async def rebalance(self, target_weights: Dict[str, float], portfolio_value: float, market_data: dict[str, float]) -> None:
        """
        Rebalance the portfolio to target weights
        
        Args:
            target_weights: Dictionary of {symbol: weight} (e.g., {'AAPL': 0.3, 'MSFT': 0.3})
            portfolio_value: Current total value of portfolio
            market_data: Dict of {symbol: price} (e.g., {'AAPL': 200.03, ...})
        """
        try:
            if target_weights is None:
                logger.info("No signal update this bar")
                return
            
            # Get current positions
            current_positions = await self.get_positions()
            all_symbols = set(target_weights.keys()) | set(current_positions.keys())
            
            # Maintain cash buffer
            portfolio_value = portfolio_value * 0.95    # placeholder
            total_weight = sum(target_weights.values()) # should < 1

            prices = {snapshot['symbol']: snapshot['price'] for snapshot in market_data.values()}
            
            # Calculate orders to place
            orders_to_place = []
            drawdown_safety = 0.99  # placeholder
            
            for symbol, target_weight in target_weights.items():
                target_value = portfolio_value * target_weight * drawdown_safety
                
                if symbol not in prices:
                    logger.warning(f"Skipping {symbol} - no price data")
                    continue
                
                price = prices[symbol]
                target_quantity = int(target_value / price)
                current_quantity = current_positions.get(symbol, 0)
                amount = target_quantity - current_quantity
                
                if amount > 0:
                    orders_to_place.append((symbol, abs(amount), "BUY"))
                elif amount < 0:
                    orders_to_place.append((symbol, abs(amount), "SELL"))
            
            # Handle positions that need to be sold (not in target weights)
            for symbol in current_positions:
                if symbol not in target_weights:
                    quantity = abs(current_positions[symbol])
                    if quantity > 0:
                        orders_to_place.append((symbol, int(quantity), "SELL"))
            
            # Separate sell and buy orders
            sell_orders = [(symbol, qty, side) for symbol, qty, side in orders_to_place if side == "SELL"]
            buy_orders = [(symbol, qty, side) for symbol, qty, side in orders_to_place if side == "BUY"]
            
            # Execute sell orders first
            for symbol, quantity, side in sell_orders:
                if quantity > 0:
                    logger.info(f"Placing order: {side} {quantity} {symbol}")
                    await self.place_order(symbol, quantity, side)
                    await asyncio.sleep(0.1)  # Small delay between orders
            
            # Execute buy orders
            for symbol, quantity, side in buy_orders:
                if quantity > 0:
                    logger.info(f"Placing order: {side} {quantity} {symbol}")
                    await self.place_order(symbol, quantity, side)
                    await asyncio.sleep(0.1)  # Small delay between orders
            
            logger.info(f"Rebalance complete: {len(sell_orders)} sell orders, {len(buy_orders)} buy orders")
            
        except Exception as e:
            logger.error(f"Error during rebalancing: {e}")
            raise
    
    async def liquidate_portfolio(self) -> None:
        try:
            positions = await self.get_positions()
            
            if not positions:
                logger.info("No positions to liquidate")
                return
            
            logger.info(f"Liquidating {len(positions)} positions: {positions}")
            
            for symbol, quantity in positions.items():
                abs_quantity = abs(quantity)
                if abs_quantity > 0:
                    logger.info(f"Liquidating {abs_quantity} shares of {symbol} (current: {quantity})")
                    await self.place_order(symbol, int(abs_quantity), "SELL")
                    await asyncio.sleep(0.1) # small delay between
            
            logger.info("Liquidation complete, all positions sold")
            
        except Exception as e:
            logger.error(f"Error during liquidation: {e}")
            raise