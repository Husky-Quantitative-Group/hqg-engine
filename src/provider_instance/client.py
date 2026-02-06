""" This is to interact with the provider API from the engine, and improve readability :) """

import httpx
import json
import os
import logging

logger = logging.getLogger(__name__)

class ProviderApiClient:
    def __init__(self):

        self.client = httpx.AsyncClient(
            base_url=os.environ.get("PROVIDER_API_URL"),
            timeout=httpx.Timeout(None, connect=10.0),
        )
    
    async def get_price(self, symbol):
        response = await self.client.get(f"/market_data/price/{symbol}")
        response.raise_for_status()
        return response.json()
    
    async def stream_prices(self, symbols):
        symbols_str = ",".join(symbols)
        try:
            async with self.client.stream(
                "GET",
                "/market_data/stream",
                params={"symbols": symbols_str}
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data = json.loads(line[6:]) # remove "data: "
                        if isinstance(data, dict) and 'symbol' in data:
                            yield data
        
        except Exception as e:
            logger.error(f"Error in stream_prices: {e}", exc_info=True)
            raise
    
    async def pause_stream(self):
        response = await self.client.post("/market_data/pause")
        response.raise_for_status()
        return response.json()
    
    async def resume_stream(self):
        response = await self.client.post("/market_data/resume")
        response.raise_for_status()
        return response.json()
    
    async def clear_queue(self):
        response = await self.client.post("/market_data/clear_queue")
        response.raise_for_status()
        return response.json()
    
    async def get_account_value(self):
        response = await self.client.get("/portfolio/account_value")
        response.raise_for_status()
        return response.json()
    
    async def get_positions(self):
        response = await self.client.get("/portfolio/positions")
        response.raise_for_status()
        return response.json()
    
    async def place_order(self, symbol, quantity, side):
        response = await self.client.post(
            "/execution/order",
            params={"symbol": symbol, "quantity": quantity, "side": side}
        )
        response.raise_for_status()
        return response.json()
    
    async def rebalance(self, target_weights, portfolio_value, market_data):
        response = await self.client.post(
            "/execution/rebalance",
            json={
                "target_weights": target_weights,
                "portfolio_value": portfolio_value,
                "market_data": market_data
            }
        )
        response.raise_for_status()
        return response.json()
    
    async def liquidate(self):
        response = await self.client.post("/execution/liquidate")
        response.raise_for_status()
        return response.json()
    
    async def close(self):  
        await self.client.aclose()

provider_client = ProviderApiClient()