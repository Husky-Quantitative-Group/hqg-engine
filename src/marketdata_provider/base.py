from abc import ABC, abstractmethod

class MarketData(ABC):
    """Abstract the interface for streaming market data"""
    
    @abstractmethod
    def __init__(self, connection):
        pass
    
    @abstractmethod
    async def get_price(self, symbol: str):
        pass
    
    @abstractmethod
    async def stream_prices(self, symbols: list[str]):
        pass
    
    @abstractmethod
    async def pause_stream(self):
        pass

    @abstractmethod
    async def resume_stream(self):
        pass

    @abstractmethod
    async def clear_queue(self):
        pass

    @abstractmethod
    async def cleanup(self):
        pass