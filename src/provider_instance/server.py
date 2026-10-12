import os
import secrets
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from src.control.provider import Provider
from src.control.runtime import Runtime
from src.control.transport import brokers_from_environment


class Context(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    account_id: str
    mode: str
    boot_id: str
    worker_session: str
    control_version: int
    configuration_digest: str


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    context: Context
    operation_id: str
    symbol: str
    quantity: float = Field(gt=0, allow_inf_nan=False)
    side: str


class Rebalance(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    context: Context
    operation_id: str
    target_weights: dict[str, float]


class StrategyFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    context: Context
    message: str = Field(max_length=500)


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
    )


def create_app(provider=None, worker_secret=None, background=True):
    @asynccontextmanager
    async def lifespan(app):
        app.state.provider = provider or from_environment()
        app.state.worker_secret = worker_secret or os.environ["PROVIDER_WORKER_SECRET"]
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

    @app.get("/internal/broker/{mode}/{resource}")
    async def broker_read(mode: str, resource: str, symbols: str = ""):
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
        return await getattr(broker, resource)(tickers)

    @app.post("/internal/order")
    async def order(body: Order):
        try:
            return await app.state.provider.submit(
                body.context.model_dump(),
                body.operation_id,
                body.symbol,
                body.quantity,
                body.side,
            )
        except HTTPException:
            raise
        except Exception:
            app.state.provider.close(("paper", "live"), "runtime_database_unavailable")
            raise HTTPException(503, "Execution unavailable")

    @app.post("/internal/rebalance")
    async def rebalance(body: Rebalance):
        try:
            return await app.state.provider.rebalance(
                body.context.model_dump(), body.operation_id, body.target_weights
            )
        except HTTPException:
            raise
        except Exception:
            app.state.provider.close(("paper", "live"), "runtime_database_unavailable")
            raise HTTPException(503, "Execution unavailable")

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

    # These paths previously reached the ungated SDK directly.
    @app.post("/execution/{operation}")
    async def legacy(operation: str):
        raise HTTPException(
            410, "Use dashboard account control and authenticated execution contexts"
        )

    return app


app = create_app()
