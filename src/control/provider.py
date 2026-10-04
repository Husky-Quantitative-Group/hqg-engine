"""The credential-owning process is the only authority that admits orders."""

import asyncio
import copy
import hashlib
import math
import time
from uuid import uuid4, UUID

import httpx
from fastapi import HTTPException

from src.control.contract import (
    MODES,
    APPROVED,
    IMAGE_DIGEST,
    digest,
    validate_configuration,
)
from src.control.transport import local_client

TERMINAL = {"filled", "canceled", "expired", "rejected"}


class Provider:
    def __init__(
        self,
        account_id,
        engine_id,
        secret,
        dashboard_url,
        brokers,
        runtime,
        interval=5,
        timeout=3,
        ttl=15,
    ):
        self.account_id, self.engine_id = str(UUID(account_id)), str(UUID(engine_id))
        if (
            set(brokers) != set(MODES)
            or not secret
            or not 0 < timeout < ttl
            or not 0 < interval < ttl
        ):
            raise ValueError("Both broker contexts and valid sync timing required")

        self.secret, self.brokers, self.runtime = secret, brokers, runtime
        self.interval, self.timeout, self.max_ttl = interval, timeout, ttl
        self.dashboard = local_client(
            dashboard_url, timeout, {"Authorization": "Bearer " + secret}
        )
        self.boot_id, self.worker_session = str(uuid4()), None
        self.deadline, self.healthy = 0, False
        self.contexts_verified, self.sync_error = False, None
        self.version, self.revision, self.configuration = 0, 0, None
        self.modes = {
            m: {
                "state": "unconfigured",
                "gate_open": False,
                "incident_id": str(uuid4()),
                "stop_reason": "provider_startup",
                "incident_version": 0,
                "reconciled": False,
                "outstanding_orders": [],
                "order_outcomes": [],
                "last_successful_cycle": None,
            }
            for m in MODES
        }
        self.sync_lock = asyncio.Lock()
        self.rebalance_locks = {m: asyncio.Lock() for m in MODES}
        self.order_locks = {m: asyncio.Lock() for m in MODES}
        self.tasks = []

    def state(self):
        return dict(
            account_id=self.account_id,
            engine_instance_id=self.engine_id,
            boot_id=self.boot_id,
            worker_session=self.worker_session,
            control_version=self.version,
            sync_error=self.sync_error,
            configuration_revision=self.revision,
            configuration_digest=(
                digest(self.configuration) if self.configuration else None
            ),
            configuration=self.configuration,
            modes=copy.deepcopy(self.modes),
        )

    async def persist(self):
        try:
            async with asyncio.timeout(self.timeout):
                await self.runtime.save(self.state())
        except Exception:
            self.close(MODES, "runtime_database_unavailable")
            self.healthy = False
            raise

    def close(self, modes, reason, force=False):
        # Synchronous transition: no network/DB await before gates close.
        for mode in modes:
            state = self.modes[mode]
            if state["gate_open"] or force or state["stop_reason"] != reason:
                state.update(incident_id=str(uuid4()), incident_version=self.version)

            state.update(
                gate_open=False,
                state="paused" if self.configuration else "unconfigured",
                stop_reason=reason,
                reconciled=False,
            )

    async def start(self):
        await self.persist()
        self.tasks = [
            asyncio.create_task(self.sync_loop()),
            asyncio.create_task(self.watchdog()),
            asyncio.create_task(self.cleanup_loop()),
        ]

    async def shutdown(self):
        self.close(MODES, "provider_shutdown")
        for task in self.tasks:
            task.cancel()
     
        await asyncio.gather(*self.tasks, return_exceptions=True)

        try:
            await self.persist()
        finally:
            await self.dashboard.aclose()
            for broker in self.brokers.values():
                await broker.close()

    async def sync_loop(self):
        while True:
            started = time.monotonic()
            await self.sync_once()
            await asyncio.sleep(max(0, self.interval - (time.monotonic() - started)))

    async def watchdog(self):
        while True:
            await asyncio.sleep(min(0.1, self.interval / 4))

            if self.healthy and time.monotonic() >= self.deadline:
                self.healthy = False
                self.close(MODES, "permission_expired")
                # Persistence is best effort; failure cannot delay the in-memory stop.
                try:
                    await self.persist()
                except Exception:
                    pass

    async def sync_once(self):
        async with self.sync_lock:
            try:
                if not self.contexts_verified:
                    for mode, broker in self.brokers.items():
                        account = await broker.request("GET", "v2/account")
                        if account["id"] != f"mock-{mode}":
                            raise ValueError("Broker account/mode binding mismatch")
                    self.contexts_verified = True
    
                request_id, sent = str(uuid4()), time.monotonic()
                body = self.state()
                body.pop("configuration")
                body.update(
                    request_id=request_id,
                    image_digest=IMAGE_DIGEST,
                    approved_strategies=APPROVED,
                )

                async with asyncio.timeout(self.timeout):
                    response = await self.dashboard.post(
                        f"internal/v1/accounts/{self.account_id}/sync", json=body
                    )
                    response.raise_for_status()
                    data = response.json()
                ttl = data["permission_ttl_seconds"]

                if (
                    data["account_id"] != self.account_id
                    or data["boot_id"] != self.boot_id
                    or data["request_id"] != request_id
                    or data["dashboard_ready"] is not True
                    or type(ttl) not in (float, int)
                    or not 0 < ttl <= self.max_ttl
                    or time.monotonic() >= sent + min(ttl, self.timeout)
                    or type(data["control_version"]) is not int
                    or data["control_version"] < self.version
                    or type(data["configuration_revision"]) is not int
                    or data["configuration_revision"] < self.revision
                    or set(data["modes"]) != set(MODES)
                ):
                    raise ValueError("Unhealthy, stale or mismatched sync")

                for desired in data["modes"].values():
                    if (
                        desired["desired_state"] not in ("running", "paused")
                        or type(desired["stop_version"]) is not int
                        or not 0 <= desired["stop_version"] <= data["control_version"]
                    ):
                        raise ValueError("Invalid desired state")

                    if desired["desired_state"] == "running":
                        UUID(desired["resume_incident_id"])
                        if (
                            type(desired["resume_version"]) is not int
                            or not desired["stop_version"]
                            < desired["resume_version"]
                            <= data["control_version"]
                        ):
                            raise ValueError("Invalid Resume version")

                    elif desired["resume_incident_id"] is not None:
                        raise ValueError("Paused mode cannot carry Resume permission")

                config = data["configuration"]
                if config is not None:
                    validate_configuration(config)
                    if digest(config) != data["configuration_digest"]:
                        raise ValueError("Configuration digest mismatch")
                elif (
                    data["configuration_digest"] is not None
                    or data["configuration_revision"] != 0
                ):
                    raise ValueError("Missing configuration")

                self.sync_error = None
                changed = data["configuration_revision"] != self.revision

                if not changed and config != self.configuration:
                    raise ValueError("Immutable revision changed")
                if changed and any(
                    s["gate_open"] or not s["reconciled"] for s in self.modes.values()
                ):
                    raise ValueError("Configuration requires reconciled pause")

                self.version = data["control_version"]
                if changed:
                    self.close(MODES, "configuration_changed", force=True)
                    self.configuration, self.revision = (
                        config,
                        data["configuration_revision"],
                    )

                self.deadline, self.healthy = sent + ttl, True
                candidates = {}
                for mode in MODES:
                    state, desired = self.modes[mode], data["modes"][mode]
                    enabled = config is not None and config["modes"][mode]["enabled"]
                    if not enabled:
                        if state["gate_open"]:
                            self.close((mode,), "mode_disabled")
                        state["state"] = "unconfigured"
                    elif desired["desired_state"] == "paused":
                        state["state"] = "paused"
                        if (
                            state["gate_open"]
                            or desired["stop_version"] > state["incident_version"]
                        ):
                            self.close((mode,), "operator_stop", force=True)
                    elif (
                        self.worker_session
                        and state["reconciled"]
                        and desired.get("resume_incident_id") == state["incident_id"]
                        and desired.get("resume_version", 0) > state["incident_version"]
                    ):
                        candidates[mode] = state["incident_id"]

                await self.persist()
                for mode, incident in candidates.items():
                    state = self.modes[mode]
                    if (
                        state["incident_id"] == incident
                        and state["reconciled"]
                        and self.healthy
                        and time.monotonic() < self.deadline
                    ):
                        state.update(gate_open=True, state="running", stop_reason=None)
            except Exception as exc:
                self.sync_error = (
                    str(exc) if isinstance(exc, ValueError) else type(exc).__name__
                )
                was_healthy = self.healthy
                self.healthy = False
                # Do not replace a closed incident on every failed retry.
                if was_healthy or any(s["gate_open"] for s in self.modes.values()):
                    self.close(MODES, "dashboard_sync_failed", force=True)
                try:
                    await self.persist()
                except Exception:
                    pass
                return False
            return True

    async def register_worker(self, session_id):
        session_id = str(UUID(session_id))
        if session_id != self.worker_session:
            self.close(MODES, "worker_restart", force=True)
            self.worker_session = session_id
            await self.persist()
        return self.state()

    def admit(self, context):
        mode = context.get("mode")
        if mode not in MODES or context.get("account_id") != self.account_id:
            raise HTTPException(403, "Account and explicit mode required")
        
        state = self.modes[mode]
        if (
            not self.healthy
            or time.monotonic() >= self.deadline
            or not state["gate_open"]
            or context.get("worker_session") != self.worker_session
            or context.get("boot_id") != self.boot_id
            or context.get("control_version") != self.version
            or context.get("configuration_digest")
            != (digest(self.configuration) if self.configuration else None)
        ):
            if time.monotonic() >= self.deadline and self.healthy:
                self.healthy = False
                self.close(MODES, "permission_expired")
            raise HTTPException(409, "Execution blocked or stale execution context")
        return mode

    async def submit(self, context, operation_id, symbol, quantity, side):
        mode = self.admit(context)
        if (
            not symbol
            or side not in ("buy", "sell")
            or not math.isfinite(quantity)
            or quantity <= 0
        ):
            raise HTTPException(400, "Invalid order")
        operation_id = str(UUID(operation_id))
        scoped = f"{self.account_id}:{mode}:{context['worker_session']}:{context['control_version']}:{operation_id}:{symbol}"
        client_id = "hqg-" + hashlib.sha256(scoped.encode()).hexdigest()[:40]
        payload = {
            "symbol": symbol,
            "qty": str(quantity),
            "side": side,
            "type": "market",
            "time_in_force": "day",
            "client_order_id": client_id,
        }
        async with self.order_locks[mode]:
            existing = await self.runtime.order(client_id)
            if existing:
                if existing["request"] != payload:
                    raise HTTPException(409, "Operation identity reused")
                # A duplicate never retries submission, even if the outcome is uncertain.
                return existing

            await self.runtime.save_order(
                mode,
                client_id,
                {
                    "request": payload,
                    "status": "uncertain",
                    "control_version": self.version,
                    "worker_session": self.worker_session,
                },
            )
            try:
                # Recheck after waiting for locks and database I/O, immediately before HTTP.
                self.admit(context)
            except HTTPException:
                await self.runtime.save_order(
                    mode, client_id, {"request": payload, "status": "discarded"}
                )
                raise
            try:

                async def final_admission(event, _info):
                    # httpcore invokes this after connection/pool waits, just before writing.
                    if event == "http11.send_request_headers.started":
                        self.admit(context)

                order = await self.brokers[mode].request(
                    "POST",
                    "v2/orders",
                    json=payload,
                    extensions={"trace": final_admission},
                )
                details = {
                    "request": payload,
                    "status": order["status"],
                    "broker_order": order,
                }

                await self.runtime.save_order(mode, client_id, details)
                self.modes[mode]["order_outcomes"] = [
                    {
                        "client_order_id": client_id,
                        "status": order["status"],
                        "filled_qty": order.get("filled_qty", "0"),
                    }
                ]
                return details
            except HTTPException:
                await self.runtime.save_order(
                    mode, client_id, {"request": payload, "status": "discarded"}
                )
                raise
            except Exception as exc:
                if isinstance(
                    exc, httpx.HTTPStatusError
                ) and exc.response.status_code in (400, 401, 403, 422):
                    await self.runtime.save_order(
                        mode, client_id, {"request": payload, "status": "rejected"}
                    )
                self.healthy = False
                self.close(MODES, "broker_outcome_uncertain", force=True)
                try:
                    await self.persist()
                except Exception:
                    pass
                raise HTTPException(
                    503, "Order outcome uncertain; reconciliation required"
                )

    async def rebalance(self, context, operation_id, weights):
        mode = self.admit(context)
        async with self.rebalance_locks[mode]:
            return await self._rebalance(context, operation_id, weights)

    async def _rebalance(self, context, operation_id, weights):
        mode = self.admit(context)
        operation_id = str(UUID(operation_id))

        if (
            not weights
            or any(not math.isfinite(w) or not 0 <= w <= 1 for w in weights.values())
            or sum(weights.values()) > 1
        ):
            raise HTTPException(400, "Invalid target weights")

        # Idempotent whole-operation result prevents recalculation from newly filled positions.
        operation_key = (
            "rebalance-"
            + hashlib.sha256(f"{mode}:{context}:{operation_id}".encode()).hexdigest()[
                :36
            ]
        )

        existing = await self.runtime.order(operation_key)
        if existing:
            if existing["weights"] != weights:
                raise HTTPException(409, "Operation identity reused")
            return existing

        broker = self.brokers[mode]
        open_orders = await broker.request(
            "GET", "v2/orders", params={"status": "open"}
        )

        if open_orders:
            raise HTTPException(
                409, "Pending orders must reconcile before another rebalance"
            )

        account = await broker.request("GET", "v2/account")
        positions = {
            p["symbol"]: float(p["qty"])
            for p in await broker.request("GET", "v2/positions")
        }

        prices = await broker.quotes(sorted(set(weights) | set(positions)))
        orders = []
        for symbol in sorted(set(weights) | set(positions)):
            qty = round(
                float(account["equity"]) * weights.get(symbol, 0) / prices[symbol]
                - positions.get(symbol, 0),
                6,
            )
            if abs(qty) > 0.000001:
                orders.append((symbol, abs(qty), "buy" if qty > 0 else "sell"))

        record = {"weights": weights, "status": "in_progress", "orders": orders}
        await self.runtime.save_order(mode, operation_key, record)

        for symbol, qty, side in sorted(orders, key=lambda o: o[2] == "buy"):
            await self.submit(context, operation_id, symbol, qty, side)

        record["status"] = "completed"

        await self.runtime.save_order(mode, operation_key, record)
        await self.runtime.cycle(mode, weights, float(account["equity"]))

        self.modes[mode]["last_successful_cycle"] = time.time()

        await self.persist()
        return record

    async def cleanup_loop(self):
        while True:
            for mode in MODES:
                if not self.modes[mode]["gate_open"]:
                    try:
                        await self.reconcile(mode)
                    except Exception:
                        self.modes[mode]["reconciled"] = False
                        try:
                            await self.persist()
                        except Exception:
                            pass
            await asyncio.sleep(min(1, self.interval))

    async def reconcile(self, mode):
        state, broker = self.modes[mode], self.brokers[mode]

        if state["gate_open"]:
            return

        incident = state["incident_id"]
        state["reconciled"] = False
        outstanding, outcomes = [], []
        records = await self.runtime.orders(mode)

        # Serialize against in-flight submission: it may have been accepted after gate closure.
        if self.order_locks[mode].locked():
            state.update(reconciled=False, outstanding_orders=["submission_in_flight"])
            return

        open_orders = await broker.request(
            "GET", "v2/orders", params={"status": "open"}
        )

        by_client = {order["client_order_id"]: order for order in open_orders}
        for client_id, details in records.items():
            if client_id.startswith("rebalance-") or details["status"] == "discarded":
                continue

            if details["status"] == "rejected" and "broker_order" not in details:
                outcomes.append(
                    {
                        "client_order_id": client_id,
                        "status": "rejected",
                        "filled_qty": "0",
                    }
                )
                continue

            try:
                order = by_client.get(client_id)
                if order is None:
                    order = await broker.request(
                        "GET",
                        "v2/orders:by_client_order_id",
                        params={"client_order_id": client_id},
                    )

                if order["status"] not in TERMINAL:
                    try:
                        await broker.request("DELETE", "v2/orders/" + order["id"])
                    finally:
                        order = await broker.request("GET", "v2/orders/" + order["id"])

                await self.runtime.save_order(
                    mode,
                    client_id,
                    {**details, "status": order["status"], "broker_order": order},
                )
                outcomes.append(
                    {
                        "client_order_id": client_id,
                        "status": order["status"],
                        "filled_qty": order.get("filled_qty", "0"),
                    }
                )

                if order["status"] not in TERMINAL:
                    outstanding.append(
                        {"client_order_id": client_id, "status": order["status"]}
                    )
            except Exception:
                outstanding.append(
                    {"client_order_id": client_id, "status": "uncertain"}
                )
        # Include engine-owned orders recovered from broker even if the database lost an ID.
        for client_id, order in by_client.items():
            if client_id.startswith("hqg-") and client_id not in records:
                await self.runtime.save_order(
                    mode,
                    client_id,
                    {"status": order["status"], "broker_order": order, "request": {}},
                )
                outstanding.append(
                    {"client_order_id": client_id, "status": "recovered"}
                )

        if state["incident_id"] == incident:
            state.update(
                reconciled=not outstanding,
                outstanding_orders=outstanding,
                order_outcomes=outcomes[-10:],
            )

        await self.persist()
