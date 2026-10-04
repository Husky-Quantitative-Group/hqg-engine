"""PostgreSQL owns execution incidents, portfolio mappings and order identities."""

import asyncio
import copy
from datetime import datetime

from sqlalchemy import select
from src.db.models import (
    Portfolio,
    ControlRuntime,
    BrokerOrder,
    AllocationEvent,
    PerformanceSnapshot,
    ExecutionEvent,
    Action,
)


class Runtime:
    def __init__(self, sessions, account_id):
        self.sessions, self.account_id = sessions, account_id
        self.lock = asyncio.Lock()

    async def save(self, state):
        snapshot = copy.deepcopy(state)
        
        async with asyncio.timeout(
            3
        ), self.lock, self.sessions() as session, session.begin():
            record = await session.get(ControlRuntime, self.account_id)
            if record is None:
                session.add(ControlRuntime(account_id=self.account_id, state=snapshot))
            else:
                record.state = snapshot

            for mode, spec in (
                (snapshot.get("configuration") or {}).get("modes", {}).items()
            ):
                result = await session.execute(
                    select(Portfolio).where(
                        Portfolio.account_id == self.account_id, Portfolio.mode == mode
                    )
                )

                portfolio = result.scalar_one_or_none()
                if portfolio is None:
                    # The legacy provider was explicitly paper-only. Preserve its history.
                    if mode == "paper":
                        legacy = await session.get(Portfolio, 1)
                        if legacy is not None and legacy.account_id is None:
                            portfolio = legacy
                    if portfolio is None:
                        portfolio = Portfolio(
                            name=f"{mode.title()} portfolio", is_active=False
                        )
                        session.add(portfolio)

                    portfolio.account_id, portfolio.mode = self.account_id, mode

                portfolio.dashboard_uuid = spec["portfolio_id"]
                portfolio.is_active = (
                    False  # Legacy DB flags are never execution authority.
                )

    async def order(self, client_id):
        async with asyncio.timeout(3), self.sessions() as session:
            record = await session.get(BrokerOrder, client_id)
            return copy.deepcopy(record.details) if record else None

    async def save_order(self, mode, client_id, details):
        async with asyncio.timeout(
            3
        ), self.lock, self.sessions() as session, session.begin():
            record = await session.get(BrokerOrder, client_id)
            previous = record.details if record else {}
            old_filled = float(previous.get("broker_order", {}).get("filled_qty", 0))
            order = details.get("broker_order", {})
            filled = float(order.get("filled_qty", 0))
            if filled > old_filled:
                result = await session.execute(
                    select(Portfolio).where(
                        Portfolio.account_id == self.account_id, Portfolio.mode == mode
                    )
                )
                portfolio = result.scalar_one()
                session.add(
                    ExecutionEvent(
                        portfolio_id=portfolio.portfolio_id,
                        timestamp=datetime.utcnow(),
                        action=Action.BUY if order["side"] == "buy" else Action.SELL,
                        symbol=order["symbol"],
                        quantity=filled - old_filled,
                    )
                )

            if record is None:
                session.add(
                    BrokerOrder(
                        client_order_id=client_id,
                        account_id=self.account_id,
                        mode=mode,
                        details=details,
                    )
                )
            else:
                record.details = details

    async def orders(self, mode):
        async with asyncio.timeout(3), self.sessions() as session:
            result = await session.execute(
                select(BrokerOrder).where(
                    BrokerOrder.account_id == self.account_id, BrokerOrder.mode == mode
                )
            )

            return {
                record.client_order_id: copy.deepcopy(record.details)
                for record in result.scalars()
            }

    async def cycle(self, mode, weights, equity):
        async with asyncio.timeout(
            3
        ), self.lock, self.sessions() as session, session.begin():
            result = await session.execute(
                select(Portfolio).where(
                    Portfolio.account_id == self.account_id, Portfolio.mode == mode
                )
            )

            portfolio = result.scalar_one()
            session.add(
                AllocationEvent(
                    portfolio_id=portfolio.portfolio_id,
                    timestamp=datetime.utcnow(),
                    allocations=weights,
                )
            )

            key = {
                "portfolio_id": portfolio.portfolio_id,
                "as_of": datetime.utcnow().date(),
            }

            record = await session.get(PerformanceSnapshot, key)
            if record is None:
                session.add(PerformanceSnapshot(**key, equity=equity))
            else:
                record.equity = equity
