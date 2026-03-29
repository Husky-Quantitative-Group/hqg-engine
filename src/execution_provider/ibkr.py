import asyncio
from ib_async import IB, Stock, MarketOrder
from .base import Executor

class IBKRExecutor(Executor):
    def __init__(self, connection: IB):
        self.ib = connection
        self.order_history = [] # placeholder for order history


    def get_order_history(self): # placeholder for order history
        return self.order_history 


    async def get_account_value(self):
        """
        Get current account value
        """
        account_values = self.ib.accountValues()
        for value in account_values:
            if value.tag == 'NetLiquidation' and value.currency == 'USD':
                return float(value.value)

        raise ValueError("Could not retrieve account value")

 
    async def place_order(self, symbol, quantity, side):
        """
        Place an order from a given symbol, quantity, and side
        """

        contract = Stock(symbol, "SMART", "USD")
        contract = await self.ib.qualifyContractsAsync(contract)
        
        if not contract:
            raise ValueError(f"Could not qualify the contract for {symbol}")
        
        contract = contract[0]
        order = MarketOrder(side, quantity)
        trade = self.ib.placeOrder(contract, order)
        self.order_history.append(trade)
        
    
    async def get_positions(self):
        positions = self.ib.positions()
        
        position_dict = {}
        for position in positions:
            symbol = position.contract.symbol
            quantity = position.position
            position_dict[symbol] = quantity
        
        return position_dict
    

    async def get_order_status(self, order_id):
        trades = self.ib.trades()
        for trade in trades:
            if trade.order.orderId == order_id:
                return trade.orderStatus.status
        return None


    async def rebalance(self, target_weights, portfolio_value):
        """
        Rebalance the portfolio to the target weights
            target_weights is a dict of {symbol: weight} or None
            portfolio_value is the current value of the portfolio
        """

        if target_weights is None:
            print("No signal update this bar")
            return
        
        portfolio_value = portfolio_value * 0.95 # to maintain cash buffer
        total_weight = sum(target_weights.values())
        
        current_positions = await self.get_positions()
        all_symbols = set(target_weights.keys()) | set(current_positions.keys())
        prices = {} # placeholder for prices
        
        for symbol in all_symbols:
            contract = Stock(symbol, "SMART", "USD")
            qualified = await self.ib.qualifyContractsAsync(contract)
            
            if qualified:
                contract = qualified[0]
                ticker = self.ib.reqMktData(contract, '', False, False)
                await asyncio.sleep(0.5)
                
                price = ticker.last if ticker.last else ticker.close
                if price is None or price == 0:
                    print(f"Warning: No valid price data for {symbol}")
                    continue
                    
                prices[symbol] = price
                self.ib.cancelMktData(contract)
        
        # calculate, for each symbol, the target quantities and amount
        orders_to_place = []
        drawdown_safety = 0.99 #temporary
        
        for symbol, target_weight in target_weights.items():
            target_value = portfolio_value * target_weight * drawdown_safety
            
            if symbol not in prices:
                print(f"Warning: No price data for {symbol}")
                continue # skip if no price data (?)
            
            price = prices[symbol]
            target_quantity = target_value / price
            
            current_quantity = current_positions.get(symbol, 0)
            amount = target_quantity - current_quantity
            
            if amount > 0:
                orders_to_place.append((symbol, abs(amount), "BUY"))
            elif amount < 0:
                orders_to_place.append((symbol, abs(amount), "SELL"))
        
        # handle positions that need to be sold
        for symbol in current_positions:
            if symbol not in target_weights:
                quantity = abs(current_positions[symbol])
                if quantity > 0:
                    orders_to_place.append((symbol, quantity, "SELL"))

        sell_orders = [(symbol, qty, side) for symbol, qty, side in orders_to_place if side == "SELL"]
        buy_orders = [(symbol, qty, side) for symbol, qty, side in orders_to_place if side == "BUY"]

        for symbol, quantity, side in sell_orders:
            if quantity > 0:
                print(f"Placing order: {side} {quantity} {symbol}")
                await self.place_order(symbol, quantity, side)

        for symbol, quantity, side in buy_orders:
            if quantity > 0:
                print(f"Placing order: {side} {quantity} {symbol}")
                await self.place_order(symbol, quantity, side)
        
        print(f"{len(sell_orders)} sell orders were placed")
        print(f"{len(buy_orders)} buy orders were placed")