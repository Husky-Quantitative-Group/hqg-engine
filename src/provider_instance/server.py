import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from src.control.provider import Provider
from src.control.runtime import Runtime
from src.control.transport import brokers_from_environment
from src.control.alternative import AlternativeService


class Context(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    account_id: str
    mode: str
    boot_id: str
    worker_session: str
    control_version: int
    configuration_digest: str


class StrategyTarget(BaseModel):
    # Reject anything the database would refuse after orders were already sent.
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    strategy_id: str = Field(max_length=80)
    version: int
    source_digest: str = Field(max_length=71)
    allocation: float
    bar_timestamp: AwareDatetime = Field(strict=False)  # ISO 8601 text in JSON
    target_weights: dict[str, float]


class Rebalance(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    context: Context
    operation_id: str
    target_weights: dict[str, float]
    strategy_targets: list[StrategyTarget] = []


class StrategyFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    context: Context
    message: str = Field(max_length=500)


class AlternativeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    context: Context
    series: list[str] = Field(min_length=1, max_length=1000)


def from_environment():
    from src.database import async_session

    account_id = os.environ["HQG_ACCOUNT_ID"]
    brokers = brokers_from_environment()

    return Provider(
        account_id,
        os.environ["HQG_ENGINE_ID"],
        os.environ["DASHBOARD_SYNC_SECRET"],
        os.environ["DASHBOARD_SYNC_URL"],
        brokers,
        Runtime(async_session, account_id),
        dashboard_proxy=os.environ.get("DASHBOARD_PROXY_URL", "").strip() or None,
    )


def create_app(provider=None, worker_secret=None, background=True, alternative=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.provider = provider or from_environment()
        app.state.worker_secret = worker_secret or os.environ["PROVIDER_WORKER_SECRET"]
        app.state.alternative = alternative or AlternativeService()
        if background:
            await app.state.provider.start()

        yield
        await app.state.provider.shutdown()

    app = FastAPI(title="Account provider", lifespan=lifespan)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if request.url.path != "/health":
            expected = "Bearer " + app.state.worker_secret
            if not secrets.compare_digest(
                request.headers.get("authorization", ""), expected
            ):
                return JSONResponse(
                    {"detail": "Worker authentication required"}, status_code=403
                )

        return await call_next(request)

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    @app.get("/internal/state")
    async def state():
        return app.state.provider.state()

    @app.post("/internal/sessions/{session_id}")
    async def register(session_id: UUID):
        return await app.state.provider.register_worker(str(session_id))

    # Registered before the generic broker read so "history" never reaches it.
    @app.get("/internal/broker/{mode}/history")
    async def broker_history(mode: str, symbols: str, bar_size: str, before: str, count: int):
        broker = app.state.provider.brokers.get(mode)
        if broker is None:
            raise HTTPException(409, "Alpaca mode is not configured")
        tickers = symbols.split(",") if symbols else []
        if not tickers or len(tickers) > 200:
            raise HTTPException(400, "1-200 symbols required")
        if bar_size not in {"1d", "1w", "1m", "1q"} or not 1 <= count <= 1000:
            raise HTTPException(400, "Unsupported lookback history")
        try:
            end = datetime.fromisoformat(before)
        except ValueError:
            end = None
        if end is None or end.tzinfo is None:
            raise HTTPException(400, "A timezone-aware bar timestamp is required")
        return await broker.history(tickers, bar_size, end, count)

    @app.get("/internal/broker/{mode}/{resource}")
    async def broker_read(
        mode: str, resource: str, symbols: str = "",
        bar_size: str = "1d", execution: str = "close_to_close",
    ):
        broker = app.state.provider.brokers.get(mode)
        if broker is None:
            raise HTTPException(409, "Alpaca mode is not configured")
        if resource in {"account", "positions"}:
            return await broker.request("GET", "v2/" + resource)
        if resource not in {"quotes", "bars"}:
            raise HTTPException(404, "Unknown broker read")
        tickers = symbols.split(",") if symbols else []
        if not tickers or len(tickers) > 200:
            raise HTTPException(400, "1-200 symbols required")
        if resource == "bars":
            if bar_size not in {"1d", "1w", "1m", "1q"} or execution not in {
                "close_to_close", "close_to_next_open",
            }:
                raise HTTPException(400, "Unsupported strategy cadence")
            return await broker.bars(tickers, bar_size, execution)
        return await broker.quotes(tickers)

    @app.post("/internal/rebalance")
    async def rebalance(body: Rebalance):
        try:
            return await app.state.provider.rebalance(
                body.context.model_dump(), body.operation_id, body.target_weights,
                [target.model_dump() for target in body.strategy_targets],
            )
        except HTTPException:
            raise
        except Exception:
            app.state.provider.close(("paper", "live"), "runtime_database_unavailable")
            raise HTTPException(503, "Execution unavailable")

    @app.post("/internal/alternative-data")
    async def alternative_data(body: AlternativeRequest):
        context = body.context.model_dump()
        app.state.provider.admit(context)
        try:
            result = await app.state.alternative.fetch(
                app.state.provider.state(), context['mode'], body.series,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        app.state.provider.admit(context)
        return result

    @app.post("/internal/failure")
    async def strategy_failure(body: StrategyFailure):
        context = body.context.model_dump()
        app.state.provider.admit(context)
        app.state.provider.close(
            (context["mode"],), "strategy_execution_failed", force=True
        )
        app.state.provider.modes[context["mode"]]["strategy_error"] = body.message
        await app.state.provider.persist()
        return {"state": "paused"}

    return app


app = create_app()
