import os
from typing import AsyncGenerator
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from src.db import Base

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
        await conn.run_sync(Base.metadata.create_all())
    print("Initialized database")

async def close_db():
    await engine.dispose()
    print("Closed database connection")