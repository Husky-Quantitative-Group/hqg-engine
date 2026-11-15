"""Reconciliation asks the broker about an order only until its outcome is final."""

import asyncio
from uuid import uuid4

from src.control.provider import Provider


class Runtime:
    def __init__(self, records):
        self.records = records

    async def save(self, state):
        pass

    async def orders(self, mode):
        return {client_id: dict(details) for client_id, details in self.records.items()}

    async def save_order(self, mode, client_id, details):
        self.records[client_id] = details


class Broker:
    account_id = None

    def __init__(self):
        self.requests = []

    async def request(self, method, path, params=None, json=None, before_send=None):
        self.requests.append(path)
        if path == "v2/orders":
            return []  # Nothing is open.
        return {"id": str(uuid4()), "status": "filled", "filled_qty": "3"}


def test_reconcile_stops_looking_up_orders_once_they_are_final():
    records = {
        "hqg-old": {"status": "filled", "broker_order": {"filled_qty": "2"}},
        "hqg-new": {"status": "pending_new", "broker_order": {"filled_qty": "0"}},
    }
    broker = Broker()
    provider = Provider(
        str(uuid4()), str(uuid4()), "test-secret", "https://api.test/stage",
        {"paper": broker}, Runtime(records), dashboard=object(),
    )

    async def scenario():
        for _ in range(3):
            await provider.reconcile("paper")

    asyncio.run(scenario())
    # The just-submitted order is looked up once, the already filled one never is.
    assert broker.requests == [
        "v2/orders", "v2/orders:by_client_order_id", "v2/orders", "v2/orders",
    ]
    state = provider.modes["paper"]
    assert state["reconciled"] and not state["outstanding_orders"]
    assert state["order_outcomes"] == [
        {"client_order_id": "hqg-old", "status": "filled", "filled_qty": "2"},
        {"client_order_id": "hqg-new", "status": "filled", "filled_qty": "3"},
    ]
