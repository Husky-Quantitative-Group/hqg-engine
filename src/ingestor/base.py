from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, Any

class MarketDataProvider(ABC):
    """Abstract the interface for streaming market data"""
    
    @abstractmethod
    def __init__(self, connection):
        pass
    
    @abstractmethod
    async def get_price(self, symbol: str):
        pass
    
    @abstractmethod
    async def stream_prices(self, symbols: list[str], interval: float):
        pass
    
    @abstractmethod
    async def cleanup(self):
        pass

