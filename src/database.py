import os
from typing import AsyncGenerator
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from src.db import Base
from sqlalchemy import text
import asyncio
import logging

logger = logging.getLogger(__name__)

engine = create_async_engine(
    os.getenv("DATABASE_URL"),
    echo=True, # dev
    # disable pooling if going w/ serverless (?)
)

async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
)

async def get_session():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

async def init_db():
    async with engine.begin() as conn:
        def create_tables(sync_conn):
            Base.metadata.create_all(bind=sync_conn)
        await conn.run_sync(create_tables)
    print("Initialized database")

async def close_db():
    await engine.dispose()
    print("Closed database connection")

async def wait_for_postgres():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL environment variable not set")
        raise ValueError
    
    check_engine = create_async_engine(db_url)
    max_retries = 60
    retry = 0
    
    while retry < max_retries:
        try:
            async with check_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("PostgreSQL is ready")
            await check_engine.dispose()
            return
        except Exception as e:
            retry += 1
            if retry >= max_retries:
                await check_engine.dispose()
                raise ConnectionError(f"Failed to connect to PostgreSQL after {max_retries} retries: {e}")
            await asyncio.sleep(1)
    
    await check_engine.dispose()

async def main():
    print("Waiting for PostgreSQL to be ready")
    await wait_for_postgres()
    
    print("Initializing database")
    await init_db()
    print("Database initialized successfully")

if __name__ == "__main__":
    asyncio.run(main())