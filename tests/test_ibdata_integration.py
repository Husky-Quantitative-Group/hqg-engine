# usage: python -m pytest -m ibkr_live .\tests\test_ibdata_integration.py -s
# expected runtime: < 5s

import os
import math
import asyncio
import pytest
from numbers import Number
from dotenv import load_dotenv
from ib_async import IB
from src.ingestor.ibkr import IBData

load_dotenv()
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1"))


@pytest.mark.asyncio
@pytest.mark.ibkr_live
async def test_ib_get_price():
    ib = IB()
    await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
    assert ib.isConnected(), f"Failed to connect to IBKR at {IB_HOST}:{IB_PORT}"

    provider = IBData(ib)
    symbol = "AAPL"

    result = await provider.get_price(symbol)
    #print(result)

    assert isinstance(result, dict)
    assert result["symbol"] == symbol

    price = result["price"]
    assert isinstance(price, Number)
    assert price > 0
    assert not (isinstance(price, float) and math.isnan(price))

    for key in ["bid", "ask", "volume", "timestamp"]:
        assert key in result
    await provider.cleanup()
    
    ib.disconnect()
    assert not ib.isConnected()



@pytest.mark.asyncio
@pytest.mark.ibkr_live
async def test_ib_stream():
    ib = IB()

    await ib.connectAsync(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)
    assert ib.isConnected(), f"Failed to connect to IBKR at {IB_HOST}:{IB_PORT}"

    provider = IBData(ib)

    symbols = ["AAPL", "SPY"]
    stream = provider.stream_prices(symbols, 1)

    updates = []

    async def collect_from_stream():
        async for update in stream:
            updates.append(update)
            if len(updates) >= 3:
                break

    await asyncio.wait_for(collect_from_stream(), timeout=20)

    assert len(updates) >= 2

    for update in updates:
        assert isinstance(update, dict)
        assert update["symbol"] in symbols

        price = update["price"]
        assert isinstance(price, Number)
        assert price > 0
        assert not (isinstance(price, float) and math.isnan(price))

        for key in ["bid", "ask", "volume", "open", "high", "low", "close", "timestamp"]:
            assert key in update
        #print(update)

    await provider.cleanup()
    
    ib.disconnect()
    assert not ib.isConnected()

