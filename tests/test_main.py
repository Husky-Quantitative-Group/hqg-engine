import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.parametrize('startup_failure', [False, True])
def test_alternative_history_is_initialized_before_quote_stream(monkeypatch, startup_failure):
    monkeypatch.setenv('DATABASE_URL', 'postgresql+asyncpg://test:test@localhost/test')
    monkeypatch.setenv('PROVIDER_API_URL', 'http://provider-api:8000')
    monkeypatch.delenv('PROVIDER', raising=False)
    main = importlib.import_module('src.main')
    portfolio = SimpleNamespace(initialize=AsyncMock(), get_tickers=lambda: ['SPY'])
    if startup_failure:
        portfolio.initialize.side_effect = RuntimeError('invalid alternative configuration')
    streamed = []

    async def stream(universe):
        portfolio.initialize.assert_awaited_once()
        streamed.append(universe)
        for snapshot in []:
            yield snapshot

    provider = SimpleNamespace(stream_prices=stream, close=AsyncMock())
    monkeypatch.setattr(main, 'Portfolio', lambda **kwargs: portfolio)
    monkeypatch.setattr(main, 'provider_client', provider)
    if startup_failure:
        with pytest.raises(RuntimeError, match='invalid alternative configuration'):
            asyncio.run(main.run())
        assert not streamed
    else:
        asyncio.run(main.run())
        assert streamed == [['SPY']]
    portfolio.initialize.assert_awaited_once()
    provider.close.assert_awaited_once()
