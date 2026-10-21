import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from src import database


class SessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


async def consume_session(generator):
    session = await anext(generator)
    try:
        await anext(generator)
    except StopAsyncIteration:
        pass
    return session


def test_get_session_commits_and_closes(monkeypatch):
    session = Mock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    monkeypatch.setattr(database, "async_session", lambda: SessionContext(session))

    yielded = asyncio.run(consume_session(database.get_session()))

    assert yielded is session
    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()
    session.close.assert_awaited_once_with()


def test_get_session_rolls_back_and_reraises(monkeypatch):
    session = Mock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    monkeypatch.setattr(database, "async_session", lambda: SessionContext(session))

    async def exercise():
        generator = database.get_session()
        assert await anext(generator) is session
        await generator.athrow(RuntimeError("query failed"))

    with pytest.raises(RuntimeError, match="query failed"):
        asyncio.run(exercise())

    session.commit.assert_not_awaited()
    session.rollback.assert_awaited_once_with()
    session.close.assert_awaited_once_with()


def test_init_db_creates_metadata(monkeypatch, capsys):
    connection = Mock()
    connection.run_sync = AsyncMock()
    connection.execute = AsyncMock()

    @asynccontextmanager
    async def begin():
        yield connection

    engine = Mock()
    engine.begin = begin
    monkeypatch.setattr(database, "engine", engine)

    asyncio.run(database.init_db())

    connection.run_sync.assert_awaited_once()
    callback = connection.run_sync.await_args.args[0]
    sync_connection = object()
    create_all = Mock()
    monkeypatch.setattr(database.Base.metadata, "create_all", create_all)
    callback(sync_connection)
    create_all.assert_called_once_with(bind=sync_connection)
    assert capsys.readouterr().out == "Initialized database\n"


def test_close_db_disposes_engine(monkeypatch, capsys):
    engine = Mock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(database, "engine", engine)

    asyncio.run(database.close_db())

    engine.dispose.assert_awaited_once_with()
    assert capsys.readouterr().out == "Closed database connection\n"


def test_wait_for_postgres_requires_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError):
        asyncio.run(database.wait_for_postgres())


def test_wait_for_postgres_executes_health_check_and_disposes(monkeypatch):
    connection = Mock()
    connection.execute = AsyncMock()

    @asynccontextmanager
    async def begin():
        yield connection

    check_engine = Mock()
    check_engine.begin = begin
    check_engine.dispose = AsyncMock()
    monkeypatch.setattr(database, "create_async_engine", lambda url: check_engine)

    asyncio.run(database.wait_for_postgres())

    connection.execute.assert_awaited_once()
    assert str(connection.execute.await_args.args[0]) == "SELECT 1"
    check_engine.dispose.assert_awaited_once_with()


def test_wait_for_postgres_retries_transient_failure(monkeypatch):
    attempts = 0
    connection = Mock()
    connection.execute = AsyncMock()

    @asynccontextmanager
    async def begin():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("not ready")
        yield connection

    check_engine = Mock()
    check_engine.begin = begin
    check_engine.dispose = AsyncMock()
    sleep = AsyncMock()
    monkeypatch.setattr(database, "create_async_engine", lambda url: check_engine)
    monkeypatch.setattr(database.asyncio, "sleep", sleep)

    asyncio.run(database.wait_for_postgres())

    assert attempts == 2
    sleep.assert_awaited_once_with(1)
    check_engine.dispose.assert_awaited_once_with()
