# Usage: python -m pytest .\tests\test_ingestor.py -s 
# Expected runtime: < 1s

from datetime import datetime
from unittest.mock import Mock, AsyncMock
import pytest
from src.ingestor.ibkr import IBData


class FakeContract:
    def __init__(self, symbol: str):
        self.symbol = symbol


class FakeTicker:
    def __init__(
        self,
        contract=None,
        last=None,
        close=None,
        bid=None,
        ask=None,
        volume=None,
        open=None,
        high=None,
        low=None,
    ):
        self.contract = contract
        self.last = last
        self.close = close
        self.bid = bid
        self.ask = ask
        self.volume = volume
        self.open = open
        self.high = high
        self.low = low


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    # Don't wait
    async def _fast_sleep(_):
        return None
    monkeypatch.setattr("src.ingestor.ibkr.asyncio.sleep", _fast_sleep)


def test_instantiate():
    fake_ib = Mock()
    data = IBData(fake_ib)

    assert data.ib is fake_ib
    assert isinstance(data._tickers, dict)
    assert data._tickers == {}


@pytest.mark.asyncio
async def test_get_price_uses_last_price():
    fake_ib = Mock()

    contract = FakeContract("AAPL")
    fake_ib.qualifyContractsAsync = AsyncMock(return_value=[contract])

    ticker = FakeTicker(
        last=150.0,
        close=149.5,
        bid=149.9,
        ask=150.1,
        volume=1000,
    )
    fake_ib.reqMktData.return_value = ticker

    data = IBData(fake_ib)
    result = await data.get_price("AAPL")

    fake_ib.qualifyContractsAsync.assert_awaited_once()
    fake_ib.reqMktData.assert_called_once()

    assert result["symbol"] == "AAPL"
    assert result["price"] == 150.0  # uses last when available
    assert result["bid"] == 149.9
    assert result["ask"] == 150.1
    assert result["volume"] == 1000
    assert isinstance(result["timestamp"], datetime)


@pytest.mark.asyncio
async def test_get_price_falls_back_to_close_when_no_last():
    fake_ib = Mock()

    contract = FakeContract("SPY")
    fake_ib.qualifyContractsAsync = AsyncMock(return_value=[contract])

    ticker = FakeTicker(
        last=None,
        close=300.0,
        bid=299.5,
        ask=300.5,
        volume=2000,
    )
    fake_ib.reqMktData.return_value = ticker

    data = IBData(fake_ib)
    result = await data.get_price("SPY")

    assert result["symbol"] == "SPY"
    assert result["price"] == 300.0  # falls back to close
    assert result["bid"] == 299.5
    assert result["ask"] == 300.5
    assert result["volume"] == 2000
    assert isinstance(result["timestamp"], datetime)


@pytest.mark.asyncio
async def test_get_price_raises_if_no_contracts():
    fake_ib = Mock()
    fake_ib.qualifyContractsAsync = AsyncMock(return_value=[])

    data = IBData(fake_ib)

    with pytest.raises(ValueError) as exc:
        await data.get_price("INVALID")

    assert "Could not qualify the contract for INVALID" in str(exc.value)


@pytest.mark.asyncio
async def test_stream_prices_yields_quotes_for_all_symbols():
    fake_ib = Mock()

    contracts = [FakeContract("AAPL"), FakeContract("SPY")]
    fake_ib.qualifyContractsAsync = AsyncMock(return_value=contracts)

    tickers_by_symbol = {
        "AAPL": FakeTicker(
            contract=contracts[0],
            last=150.0,
            close=149.0,
            bid=149.8,
            ask=150.2,
            volume=10000,
            open=148.0,
            high=151.0,
            low=147.5,
        ),
        "SPY": FakeTicker(
            contract=contracts[1],
            last=None,         # force fallback to close
            close=300.0,
            bid=299.5,
            ask=300.5,
            volume=8000,
            open=295.0,
            high=302.0,
            low=294.0,
        ),
    }

    def reqMktData_side_effect(contract, *args, **kwargs):
        return tickers_by_symbol[contract.symbol]

    fake_ib.reqMktData.side_effect = reqMktData_side_effect

    data = IBData(fake_ib)

    # Take one "round" of quotes
    quotes = []
    async for quote in data.stream_prices(["AAPL", "SPY"]):
        quotes.append(quote)
        if len(quotes) >= 2:
            break

    symbols_returned = {q["symbol"] for q in quotes}
    assert symbols_returned == {"AAPL", "SPY"}

    aapl_quote = next(q for q in quotes if q["symbol"] == "AAPL")
    SPY_quote = next(q for q in quotes if q["symbol"] == "SPY")

    # AAPL uses last
    assert aapl_quote["price"] == 150.0
    assert aapl_quote["bid"] == 149.8
    assert aapl_quote["ask"] == 150.2
    assert aapl_quote["volume"] == 10000
    assert aapl_quote["open"] == 148.0
    assert aapl_quote["high"] == 151.0
    assert aapl_quote["low"] == 147.5
    assert aapl_quote["close"] == 149.0
    assert isinstance(aapl_quote["timestamp"], datetime)

    # SPY falls back to close
    assert SPY_quote["price"] == 300.0
    assert SPY_quote["bid"] == 299.5
    assert SPY_quote["ask"] == 300.5
    assert SPY_quote["volume"] == 8000
    assert SPY_quote["open"] == 295.0
    assert SPY_quote["high"] == 302.0
    assert SPY_quote["low"] == 294.0
    assert SPY_quote["close"] == 300.0
    assert isinstance(SPY_quote["timestamp"], datetime)

    # _tickers should be populated with the tickers we created
    assert set(data._tickers.keys()) == {"AAPL", "SPY"}


@pytest.mark.asyncio
async def test_stream_prices_raises_if_no_contracts():
    fake_ib = Mock()
    fake_ib.qualifyContractsAsync = AsyncMock(return_value=[])

    data = IBData(fake_ib)

    with pytest.raises(ValueError) as exc:
        # Need to start the async generator to hit the qualification logic
        async for _ in data.stream_prices(["AAPL", "SPY"]):
            break

    assert "Could not qualify any contracts" in str(exc.value)


@pytest.mark.asyncio
async def test_cleanup_cancels_all_tickers_and_clears_dict():
    fake_ib = Mock()

    data = IBData(fake_ib)
    contract1 = FakeContract("AAPL")
    contract2 = FakeContract("SPY")

    ticker1 = FakeTicker(contract=contract1)
    ticker2 = FakeTicker(contract=contract2)

    data._tickers = {"AAPL": ticker1, "SPY": ticker2}

    await data.cleanup()

    fake_ib.cancelMktData.assert_any_call(contract1)
    fake_ib.cancelMktData.assert_any_call(contract2)
    assert fake_ib.cancelMktData.call_count == 2

    assert data._tickers == {}
