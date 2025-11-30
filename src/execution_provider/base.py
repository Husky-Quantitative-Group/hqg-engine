from abc import ABC, abstractmethod

class Executor(ABC):
    @abstractmethod
    def __init__(self, connection):
        pass

    @abstractmethod
    def get_order_history(self):
        pass

    @abstractmethod
    async def get_account_value(self):
        pass

    @abstractmethod
    async def place_order(self, symbol, quantity, side):
        pass
        
    @abstractmethod
    async def get_positions(self):
        pass
    
    @abstractmethod
    async def get_order_status(self, order_id):
        pass

    @abstractmethod
    async def rebalance(self, target_weights, portfolio_value):
        pass