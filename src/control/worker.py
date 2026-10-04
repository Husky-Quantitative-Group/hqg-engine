import asyncio
import os
from uuid import uuid4

from src.control.contract import MODES, REGISTRY
from src.control.transport import local_client, Broker


class Worker:
    def __init__(self, provider_url, secret, brokers):
        self.client = local_client(
            provider_url, headers={"Authorization": "Bearer " + secret}
        )
        self.session_id = str(uuid4())
        self.brokers = brokers
        self.strategies, self.revision = {}, None

    async def register(self):
        for attempt in range(30):
            try:
                response = await self.client.post(
                    "internal/sessions/" + self.session_id
                )
                response.raise_for_status()
                return
            except Exception:
                if attempt == 29:
                    raise
                await asyncio.sleep(1)

    async def cycle(self, mode):
        response = await self.client.get("internal/state")
        response.raise_for_status()
        state = response.json()

        if state["worker_session"] != self.session_id:
            raise RuntimeError("Worker session replaced; stop the old worker")

        if state["configuration_revision"] != self.revision:
            self.strategies = {}
            for m, spec in (state["configuration"] or {}).get("modes", {}).items():
                self.strategies[m] = [
                    (
                        REGISTRY[
                            (s["strategy_id"], s["version"], s["source_digest"])
                        ](),
                        s["allocation"],
                    )
                    for s in spec["strategies"]
                ]

            self.revision = state["configuration_revision"]

        if not state["modes"][mode]["gate_open"]:
            return None

        strategies = self.strategies[mode]
        symbols = sorted({s for instance, _ in strategies for s in instance.universe})
        prices = await self.brokers[mode].quotes(symbols)
        weights = {}

        for instance, allocation in strategies:
            for symbol, weight in instance.on_data(prices).items():
                weights[symbol] = weights.get(symbol, 0) + weight * allocation

        context = {
            k: state[k]
            for k in (
                "account_id",
                "boot_id",
                "worker_session",
                "control_version",
                "configuration_digest",
            )
        }
        context["mode"] = mode
        response = await self.client.post(
            "internal/rebalance",
            json={
                "context": context,
                "operation_id": str(uuid4()),
                "target_weights": weights,
            },
        )
        response.raise_for_status()

        return response.json()

    async def mode_loop(self, mode):
        while True:
            try:
                await self.cycle(mode)
            except RuntimeError:
                raise
            except Exception:
                # The provider owns the safety gate; retry status without synthesizing permission.
                pass
            await asyncio.sleep(5)

    async def run(self):
        await self.register()
        try:
            await asyncio.gather(*(self.mode_loop(mode) for mode in MODES))
        finally:
            await self.client.aclose()
            for broker in self.brokers.values():
                await broker.close()


async def run():
    if os.environ.get("BROKER_TRANSPORT") != "mock":
        raise ValueError("Current MVP-POC requires mock transport")

    # In the mock stack workers have read-only fixtures; only the provider has mutation credentials.
    brokers = {
        m: Broker(
            m,
            os.environ[f"{m.upper()}_TRADING_URL"],
            os.environ[f"{m.upper()}_DATA_URL"],
            read_only=True,
        )
        for m in MODES
    }

    await Worker(
        os.environ["PROVIDER_API_URL"], os.environ["PROVIDER_WORKER_SECRET"], brokers
    ).run()
