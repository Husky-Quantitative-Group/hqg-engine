import asyncio
import os
import signal
from datetime import datetime
from uuid import uuid4

from src.control.contract import MODES
from src.control.transport import local_client, BrokerReader
from src.control.sandbox import Sandbox, validate_weights


class Worker:
    def __init__(self, provider_url, secret, brokers=None):
        self.client = local_client(
            provider_url, timeout=30, headers={"Authorization": "Bearer " + secret}
        )
        self.session_id = str(uuid4())
        self.brokers = brokers or {mode: BrokerReader(mode, self.client) for mode in MODES}
        self.strategies, self.revision = {}, None
        self.configuration_lock = asyncio.Lock()
        self.last_weights = {}
        self.last_bars, self.pending_rebalances = {}, {}

    async def load_configuration(self, state):
        async with self.configuration_lock:
            if state["configuration_revision"] == self.revision:
                return
            await self.close_strategies()
            strategies = {}
            for mode, spec in (state["configuration"] or {}).get("modes", {}).items():
                strategies[mode] = []
                for item in spec["strategies"]:
                    instance = Sandbox(state["strategy_packages"][item["source_digest"]])
                    if instance.identity != {
                        k: item[k] for k in ("strategy_id", "version", "source_digest")
                    }:
                        raise ValueError("Worker package identity mismatch")
                    strategies[mode].append((instance, item["allocation"]))
            self.strategies, self.revision = strategies, state["configuration_revision"]
            self.last_weights = {}
            self.last_bars, self.pending_rebalances = {}, {}

    async def close_strategies(self):
        for strategies in self.strategies.values():
            for instance, _ in strategies:
                if isinstance(instance, Sandbox):
                    await asyncio.to_thread(instance.close)

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

        if not state["modes"][mode]["gate_open"]:
            await self.load_configuration(state)
            # Stopped strategy contexts release their containers. Resume starts fresh.
            for instance, _ in self.strategies.get(mode, []):
                if isinstance(instance, Sandbox):
                    await asyncio.to_thread(instance.close)
            self.last_weights = {
                key: value for key, value in self.last_weights.items() if key[0] != mode
            }
            self.last_bars = {
                key: value for key, value in self.last_bars.items() if key[0] != mode
            }
            self.pending_rebalances.pop(mode, None)
            return None

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
        if mode in self.pending_rebalances and self.revision == state["configuration_revision"]:
            return await self.send_rebalance(context)
        try:
            await self.load_configuration(state)
            strategies = self.strategies[mode]
            broker = self.brokers[mode]
            fresh = {}
            for index, (instance, _) in enumerate(strategies):
                data = await broker.bars(
                    instance.universe, instance.cadence.bar_size.value,
                    instance.cadence.execution.value,
                )
                if not data or any(symbol not in data for symbol in instance.universe):
                    continue
                timestamps = {
                    datetime.fromisoformat(data[symbol]["timestamp"])
                    for symbol in instance.universe
                }
                if any(timestamp.tzinfo is None for timestamp in timestamps):
                    raise ValueError("Bar timestamps require a timezone")
                if len(timestamps) != 1:
                    continue  # Wait for a complete, aligned multi-symbol bar.
                timestamp = timestamps.pop()
                previous = self.last_bars.get((mode, index))
                if previous is None or timestamp > previous:
                    fresh[index] = (timestamp, {
                        symbol: {key: value for key, value in data[symbol].items() if key != "timestamp"}
                        for symbol in instance.universe
                    })
            if not fresh:
                return None
            positions = {
                p["symbol"]: float(p["qty"])
                for p in await broker.request("GET", "v2/positions")
            }
            account = await broker.request("GET", "v2/account")
            symbols = sorted(
                {s for instance, _ in strategies for s in instance.universe}
                | set(positions)
            )
            prices = await broker.quotes(symbols)
            equity = float(account["equity"])
            held_weights = {s: q * prices[s] / equity for s, q in positions.items()}
            portfolio = {
                "equity": equity,
                "cash": float(account["cash"]),
                "positions": positions,
                "weights": held_weights,
            }
            weights = {}
            all_hold, initial_hold = True, False
            for index, (instance, allocation) in enumerate(strategies):
                targets = None
                if index in fresh:
                    timestamp, data = fresh[index]
                    targets = await asyncio.to_thread(instance.on_data, data, portfolio)
                    self.last_bars[(mode, index)] = timestamp
                if targets is None:
                    initial_hold |= (mode, index) not in self.last_weights
                    targets = self.last_weights.get((mode, index), {})
                else:
                    all_hold = False
                    targets = validate_weights(targets, instance.universe)
                    self.last_weights[(mode, index)] = targets
                for symbol, weight in targets.items():
                    weights[symbol] = weights.get(symbol, 0) + weight * allocation
            # No strategy target exists yet for an initial Hold. Retain the
            # entire mode until every allocation has supplied its first target.
            if all_hold or initial_hold:
                return None
        except Exception as exc:
            # Bind failures to the same session/version as the calculation. A stale
            # failure must not close a newly resumed context.
            await self.client.post(
                "internal/failure", json={"context": context, "message": str(exc)[:500]}
            )
            raise
        return await self.send_rebalance(context, weights)

    async def send_rebalance(self, context, weights=None):
        mode = context["mode"]
        pending = self.pending_rebalances.get(mode)
        if weights is not None or pending["context"] != context:
            # Retain a calculated signal across temporary rejection without
            # evaluating its bar again. Retries reuse the same operation ID.
            pending = self.pending_rebalances[mode] = {
                "context": context,
                "operation_id": str(uuid4()),
                "target_weights": weights if weights is not None else pending["target_weights"],
            }
        response = await self.client.post(
            "internal/rebalance", json=pending,
        )
        response.raise_for_status()
        self.pending_rebalances.pop(mode, None)
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
        try:
            await self.register()
            await asyncio.gather(*(self.mode_loop(mode) for mode in MODES))
        finally:
            try:
                await self.close_strategies()
            finally:
                await self.client.aclose()
                for broker in self.brokers.values():
                    await broker.close()


async def run():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
    try:
        await Worker(
            os.environ["PROVIDER_API_URL"],
            os.environ["PROVIDER_WORKER_SECRET"],
        ).run()
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
