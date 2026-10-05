import asyncio
import copy
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, text

from conftest import Harness, ACCOUNT, HTTPServer, WORKER_SECRET
from src.control.contract import APPROVED
from src.control.transport import Broker, local_client
from src.control.worker import Worker
from src.db.models import Portfolio, AllocationEvent, ExecutionEvent


def run(coroutine):
    asyncio.run(coroutine)


def test_authorization_configuration_and_concurrent_worker_contexts(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            async with httpx.AsyncClient(
                base_url=h.dashboard.url, trust_env=False
            ) as stranger:
                assert (
                    await stranger.get(f"/accounts/{ACCOUNT}/status")
                ).status_code == 403
            body, result = await h.configure()
            duplicate = await h.user.put(
                f"/accounts/{ACCOUNT}/configuration", json=body
            )
            assert duplicate.json() == result
            changed = copy.deepcopy(body)
            changed["configuration"]["modes"]["paper"]["strategies"][0]["version"] = 99
            assert (
                await h.user.put(f"/accounts/{ACCOUNT}/configuration", json=changed)
            ).status_code == 409
            changed["idempotency_key"] = str(uuid4())
            changed["expected_control_version"] = h.provider.version
            assert (
                await h.user.put(f"/accounts/{ACCOUNT}/configuration", json=changed)
            ).status_code == 400
            worker = Worker(
                h.provider_server.url,
                WORKER_SECRET,
                {
                    m: Broker(
                        m,
                        h.broker.url + "/" + m,
                        h.broker.url + "/" + m,
                        read_only=True,
                    )
                    for m in ("paper", "live")
                },
            )
            await worker.register()
            await h.sync_clean()
            await h.resume("paper")
            await h.resume("live")
            await asyncio.gather(worker.cycle("paper"), worker.cycle("live"))
            assert (
                worker.strategies["paper"][0][0] is not worker.strategies["live"][0][0]
            )
            assert worker.strategies["paper"][0][0].cycles == 1
            async with h.sessions() as session:
                portfolios = (await session.execute(select(Portfolio))).scalars().all()
                assert {p.mode for p in portfolios} == {"paper", "live"}
                assert len({p.dashboard_uuid for p in portfolios}) == 2
                from src.routes import get_snapshot, Timeframe

                for portfolio in portfolios:
                    snapshot = await get_snapshot(
                        portfolio.portfolio_id, Timeframe.YEAR_TO_DATE, session
                    )
                    assert snapshot.account_id == ACCOUNT
                    assert snapshot.mode == portfolio.mode
                    assert snapshot.portfolio_uuid == portfolio.dashboard_uuid
                assert (
                    len(
                        (await session.execute(select(AllocationEvent))).scalars().all()
                    )
                    == 2
                )
            for mode in ("paper", "live"):
                logs = h.broker_app.state.contexts[mode]["requests"]
                assert any(r["path"] == "v2/stocks/quotes/latest" for r in logs)
                assert (
                    len(
                        [
                            r
                            for r in logs
                            if r["method"] == "POST" and r["path"] == "v2/orders"
                        ]
                    )
                    == 2
                )
            assert h.broker_app.state.contexts["paper"]["positions"]["AAPL"] == 40
            assert h.broker_app.state.contexts["live"]["positions"]["AAPL"] == 80
            no_mode = {
                "context": h.context("paper"),
                "operation_id": str(uuid4()),
                "symbol": "AAPL",
                "quantity": 1.0,
                "side": "buy",
            }
            del no_mode["context"]["mode"]
            assert (
                await h.execution.post("/internal/order", json=no_mode)
            ).status_code == 422
            no_mode["context"]["mode"] = "live"
            no_mode["context"]["account_id"] = str(uuid4())
            assert (
                await h.execution.post("/internal/order", json=no_mode)
            ).status_code == 403
            for mutation in ("order", "rebalance", "liquidate"):
                assert (
                    await h.execution.post("/execution/" + mutation)
                ).status_code == 410
            await worker.client.aclose()
            for broker in worker.brokers.values():
                await broker.close()

    run(scenario())


def test_lost_action_response_stop_acknowledgement_and_idempotency(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            h.dashboard_app.state.faults["lose_action_response"] = True
            body, response = await h.action("stop", "paper")
            assert response.status_code == 503
            assert (await h.status())["pending"]
            _, resume = await h.action("resume", "paper")
            assert resume.status_code == 409
            h.dashboard_app.state.faults["lose_action_response"] = False
            retry = await h.user.post(f"/accounts/{ACCOUNT}/actions", json=body)
            assert retry.status_code == 200
            assert (
                retry.json()["control_version"] == body["expected_control_version"] + 1
            )
            await h.sync_clean()
            assert not (await h.status())["pending"]
            assert not h.provider.modes["paper"]["gate_open"]
            await h.resume("paper")
            _, response = await h.action("stop_all")
            assert response.status_code == 200
            await h.sync_clean()
            assert all(not s["gate_open"] for s in h.provider.modes.values())
            state = h.store.get(ACCOUNT, "state")
            assert state["control_version"] == h.provider.version
            assert h.store.get(ACCOUNT, "action#" + body["idempotency_key"])
            assert h.store.get(ACCOUNT, "audit#" + body["idempotency_key"])

    run(scenario())


@pytest.mark.parametrize(
    "failure",
    [
        "backend",
        "control",
        "website",
        "generic_health",
        "delay",
        "malformed",
        "wrong_boot",
        "stale_version",
        "wrong_request",
        "wrong_account",
        "unauthorized",
    ],
)
def test_health_failure_closes_both_modes_and_requires_incident_resume(
    tmp_path, failure
):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            old_resume = await h.resume("paper")
            await h.resume("live")
            if failure == "backend":
                h.dashboard_app.state.faults["backend"] = True
            elif failure in ("control", "website", "generic_health"):
                h.readiness_app.state.faults[failure] = True
            elif failure == "delay":
                h.dashboard_app.state.faults["delay"] = 0.4
            elif failure == "unauthorized":
                h.provider.dashboard.headers["authorization"] = "Bearer FAKE-WRONG"
            else:

                async def bad_response(request):
                    body = __import__("json").loads(request.content)
                    response = await original.post(
                        f"internal/v1/accounts/{ACCOUNT}/sync", json=body
                    )
                    data = response.json()
                    if failure == "wrong_request":
                        data["request_id"] = str(uuid4())
                    if failure == "wrong_account":
                        data["account_id"] = str(uuid4())
                    if failure == "wrong_boot":
                        data["boot_id"] = str(uuid4())
                    if failure == "stale_version":
                        data["control_version"] -= 1
                    if failure == "malformed":
                        data.pop("modes")
                    return httpx.Response(200, json=data)

                original = h.provider.dashboard
                h.provider.dashboard = httpx.AsyncClient(
                    transport=httpx.MockTransport(bad_response)
                )
            assert not await h.provider.sync_once()
            assert all(not s["gate_open"] for s in h.provider.modes.values())
            assert (await h.order("paper")).status_code == 409
            h.dashboard_app.state.faults.update(backend=False, delay=0)
            h.readiness_app.state.faults.update(
                control=False, website=False, generic_health=False
            )
            if failure == "unauthorized":
                h.provider.dashboard.headers["authorization"] = (
                    "Bearer FAKE-SYNC-SECRET"
                )
            if failure in (
                "wrong_boot",
                "stale_version",
                "malformed",
                "wrong_request",
                "wrong_account",
            ):
                await h.provider.dashboard.aclose()
                h.provider.dashboard = original
            await h.sync_clean()
            assert all(not s["gate_open"] for s in h.provider.modes.values())
            _, stale = await h.action(
                "resume", "paper", incident=old_resume["incident_id"]
            )
            assert stale.status_code == 409
            await h.resume("paper")
            assert not h.provider.modes["live"]["gate_open"]

    run(scenario())


def test_watchdog_blocks_remaining_rebalance_orders_despite_broker_latency(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            h.broker_app.state.contexts["paper"]["faults"]["submission_delay"] = 0.16
            watchdog = asyncio.create_task(h.provider.watchdog())
            request = asyncio.create_task(
                h.execution.post(
                    "/internal/rebalance",
                    json={
                        "context": h.context("paper"),
                        "operation_id": str(uuid4()),
                        "target_weights": {"AAPL": 0.4, "MSFT": 0.4},
                    },
                )
            )
            for _ in range(100):
                if any(
                    r["method"] == "POST"
                    for r in h.broker_app.state.contexts["paper"]["requests"]
                ):
                    break
                await asyncio.sleep(0.005)
            h.provider.deadline = __import__("time").monotonic()
            await asyncio.sleep(0.04)
            assert not h.provider.modes["paper"]["gate_open"]
            assert (
                not request.done()
            )  # Broker I/O is still pending while the watchdog runs.
            response = await request
            assert response.status_code == 409, response.text
            assert not h.provider.modes["paper"]["gate_open"]
            submissions = [
                r
                for r in h.broker_app.state.contexts["paper"]["requests"]
                if r["method"] == "POST"
            ]
            assert len(submissions) == 1
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)

    run(scenario())


@pytest.mark.parametrize("fill_during_cancel", [False, True])
def test_cancellation_uncertainty_and_fill_are_recorded_without_liquidation(
    tmp_path, fill_during_cancel
):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            ctx = h.broker_app.state.contexts["paper"]
            ctx["faults"].update(order_status="partially_filled", cancel_failure=True)
            assert (await h.order("paper")).status_code == 200
            _, stop = await h.action("stop_all")
            assert stop.status_code == 200
            await h.sync_clean()
            assert not h.provider.modes["paper"]["reconciled"]
            assert h.provider.modes["paper"]["outstanding_orders"]
            _, response = await h.action("resume", "paper")
            assert response.status_code == 409
            ctx["faults"].update(
                cancel_failure=False, fill_on_cancel=fill_during_cancel
            )
            await h.sync_clean()
            assert h.provider.modes["paper"]["reconciled"]
            records = await h.runtime.orders("paper")
            assert {r["status"] for r in records.values()} == {
                "filled" if fill_during_cancel else "canceled"
            }
            assert ctx["positions"]["AAPL"] == (1 if fill_during_cancel else 0.5)
            async with h.sessions() as session:
                fills = (await session.execute(select(ExecutionEvent))).scalars().all()
                assert sum(float(fill.quantity) for fill in fills) == (
                    1 if fill_during_cancel else 0.5
                )
            assert h.provider.modes["paper"]["order_outcomes"][0]["status"] == (
                "filled" if fill_during_cancel else "canceled"
            )
            assert len([r for r in ctx["requests"] if r["method"] == "POST"]) == 1
            assert any(r["method"] == "DELETE" for r in ctx["requests"])

    run(scenario())


def test_timeout_after_acceptance_reconciles_by_client_id_and_discards_old_context(
    tmp_path,
):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            ctx = h.broker_app.state.contexts["paper"]
            ctx["faults"]["submission_delay"] = 0.5
            old_context = h.context("paper")
            operation = str(uuid4())
            assert (await h.order("paper", operation)).status_code == 503
            ctx["faults"].clear()
            await h.sync_clean()
            assert {
                r["status"] for r in (await h.runtime.orders("paper")).values()
            } == {"filled"}
            assert any(
                r["path"] == "v2/orders:by_client_order_id" for r in ctx["requests"]
            )
            await h.resume("paper")
            stale = await h.execution.post(
                "/internal/order",
                json={
                    "context": old_context,
                    "operation_id": operation,
                    "symbol": "AAPL",
                    "quantity": 1.0,
                    "side": "buy",
                },
            )
            assert stale.status_code == 409
            assert len([r for r in ctx["requests"] if r["method"] == "POST"]) == 1

    run(scenario())


def test_restart_conflicting_boot_database_loss_and_network_policy(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            replacement = h.new_provider()
            assert not await replacement.sync_once()
            assert all(not s["gate_open"] for s in replacement.modes.values())
            await replacement.shutdown()
            await h.execution.post("/internal/sessions/" + str(uuid4()))
            assert not h.provider.modes["paper"]["gate_open"]
            async with h.db.begin() as connection:
                await connection.execute(text("DROP TABLE control_runtime"))
            assert not await h.provider.sync_once()
            assert all(not s["gate_open"] for s in h.provider.modes.values())
            for url in (
                "https://paper-api.alpaca.markets",
                "http://example.com:80",
                "http://127.0.0.1",
                "http://localhost:1234",
            ):
                with pytest.raises(ValueError):
                    local_client(url)
            with pytest.raises(AssertionError):
                __import__("socket").getaddrinfo("api.alpaca.markets", 443)

    run(scenario())


def test_dynamodb_atomic_configuration_audit_and_idempotency(tmp_path):
    async def scenario():
        import boto3
        from moto import mock_aws
        from store import DynamoStore
        from control import Conflict
        import json

        with mock_aws():
            async with Harness(tmp_path) as h:
                client = boto3.client(
                    "dynamodb",
                    region_name="us-east-1",
                    aws_access_key_id="FAKE",
                    aws_secret_access_key="FAKE",
                )
                client.create_table(
                    TableName="accounts",
                    BillingMode="PAY_PER_REQUEST",
                    KeySchema=[
                        {"AttributeName": "account_id", "KeyType": "HASH"},
                        {"AttributeName": "record", "KeyType": "RANGE"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "account_id", "AttributeType": "S"},
                        {"AttributeName": "record", "AttributeType": "S"},
                    ],
                )
                state = h.store.get(ACCOUNT, "state")
                store = DynamoStore(client, "accounts")
                client.put_item(
                    TableName="accounts",
                    Item={
                        **store.key(ACCOUNT, "state"),
                        "generation": {"N": str(state["generation"])},
                        "body": {"S": json.dumps(state)},
                    },
                )
                h.store = store
                h.dashboard_app.state.control.store = store
                body, result = await h.configure()
                assert (
                    await h.user.put(f"/accounts/{ACCOUNT}/configuration", json=body)
                ).json() == result
                assert (
                    store.get(ACCOUNT, "revision#1")["digest"]
                    == h.provider.state()["configuration_digest"]
                )
                assert (
                    store.get(ACCOUNT, "audit#" + body["idempotency_key"])["actor"]
                    == "operator"
                )
                current = store.get(ACCOUNT, "state")
                with pytest.raises(Conflict):
                    store.commit(
                        ACCOUNT,
                        current["generation"] - 1,
                        {**current, "generation": current["generation"] + 1},
                        {"audit#invalid": {"bad": True}},
                    )
                assert store.get(ACCOUNT, "audit#invalid") is None
                assert store.get(ACCOUNT, "state") == current
                await h.resume("paper")
                assert (await h.order("paper")).status_code == 200

    run(scenario())


def test_disabled_live_and_account_service_authentication(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure(live=False)
            _, response = await h.action("resume", "live")
            assert response.status_code == 409
            assert (await h.order("live")).status_code == 409
            h.user.cookies.set("mock_operator", "paper-operator")
            # The live grant is checked before mode enablement and no action is written.
            _, response = await h.action("resume", "live")
            assert response.status_code == 403
            await h.resume("paper")
            assert (await h.order("paper")).status_code == 200
            async with httpx.AsyncClient(
                base_url=h.provider_server.url, trust_env=False
            ) as stranger:
                assert (
                    await stranger.post("/internal/sessions/" + str(uuid4()))
                ).status_code == 403
                assert (
                    await stranger.post("/internal/order", json={})
                ).status_code == 403
            response = await h.user.post(
                f"/internal/v1/accounts/{ACCOUNT}/sync", json=h.provider.state()
            )
            assert response.status_code == 403

    run(scenario())


def test_queued_order_is_discarded_after_stop(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            lock = h.provider.order_locks["paper"]
            await lock.acquire()
            queued = asyncio.create_task(h.order("paper"))
            await asyncio.sleep(0.04)
            _, response = await h.action("stop", "paper")
            assert response.status_code == 200
            assert await h.provider.sync_once()
            lock.release()
            assert (await queued).status_code == 409
            assert not any(
                r["method"] == "POST"
                for r in h.broker_app.state.contexts["paper"]["requests"]
            )
            assert {
                r["status"] for r in (await h.runtime.orders("paper")).values()
            } == {"discarded"}

    run(scenario())


def test_permission_is_rechecked_after_http_transport_wait(tmp_path):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            transport_wait = asyncio.Event()
            release = asyncio.Event()

            async def pause_before_transport(request):
                if request.method == "POST":
                    transport_wait.set()
                    await release.wait()

            h.provider.brokers["paper"].trading.event_hooks["request"].append(
                pause_before_transport
            )
            queued = asyncio.create_task(h.order("paper"))
            await transport_wait.wait()
            h.provider.deadline = __import__("time").monotonic()
            release.set()
            assert (await queued).status_code == 409
            assert all(not status["gate_open"] for status in h.provider.modes.values())
            assert not any(
                r["method"] == "POST"
                for r in h.broker_app.state.contexts["paper"]["requests"]
            )
            assert {
                r["status"] for r in (await h.runtime.orders("paper")).values()
            } == {"discarded"}

    run(scenario())


@pytest.mark.parametrize(
    "fault", ["reject", "authentication", "disconnect_after_acceptance"]
)
def test_broker_faults_reconcile_without_retrying_submission(tmp_path, fault):
    async def scenario():
        async with Harness(tmp_path) as h:
            await h.configure()
            await h.resume("paper")
            ctx = h.broker_app.state.contexts["paper"]
            ctx["faults"][fault] = True
            assert (await h.order("paper")).status_code == 503
            assert all(not status["gate_open"] for status in h.provider.modes.values())
            ctx["faults"].clear()
            await h.sync_clean()
            assert h.provider.modes["paper"]["reconciled"]
            assert {
                r["status"] for r in (await h.runtime.orders("paper")).values()
            } == {"filled" if fault == "disconnect_after_acceptance" else "rejected"}
            assert len([r for r in ctx["requests"] if r["method"] == "POST"]) == 1
            assert all(not status["gate_open"] for status in h.provider.modes.values())
            await h.resume("paper")

    run(scenario())
