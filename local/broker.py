"""Small Alpaca-shaped HTTP fixture, with isolated paper/live state and faults."""

import asyncio
from uuid import uuid4
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response


def create_app():
    app = FastAPI(title="Mock Alpaca HTTP")
    contexts = {
        m: {
            "equity": 10000 if m == "paper" else 20000,
            "positions": {},
            "orders": {},
            "quotes": {"AAPL": 100, "MSFT": 200},
            "faults": {},
            "requests": [],
        }
        for m in ("paper", "live")
    }
    app.state.contexts = contexts

    @app.post("/test/{mode}/faults")
    async def faults(mode: str, request: Request):
        contexts[mode]["faults"] = await request.json()
        return contexts[mode]["faults"]

    @app.get("/test/{mode}/requests")
    async def requests(mode: str):
        return contexts[mode]["requests"]

    @app.api_route("/{mode}/{path:path}", methods=["GET", "POST", "DELETE"])
    async def broker(mode: str, path: str, request: Request):
        if mode not in contexts:
            raise HTTPException(400, "Explicit mode required")
        context, method = contexts[mode], request.method
        faults = context["faults"]
        body = await request.json() if method == "POST" else None
        context["requests"].append(
            {
                "method": method,
                "path": path,
                "body": body,
                "query": str(request.url.query),
            }
        )
  
        key = request.headers.get("APCA-API-KEY-ID")
  
        if (
            key not in (f"FAKE-{mode}", f"FAKE-READ-{mode}")
            or request.headers.get("APCA-API-SECRET-KEY") != "FAKE-SECRET"
            or faults.get("authentication")
        ):
            raise HTTPException(401, "Fake credential required")
        if method != "GET" and key.startswith("FAKE-READ-"):
            raise HTTPException(403, "Worker has read-only broker access")
        if faults.get("malformed"):
            return Response("broken", media_type="application/json")
        if path == "v2/account" and method == "GET":
            return {"id": f"mock-{mode}", "equity": str(context["equity"])}
        if path == "v2/positions" and method == "GET":
            return [
                {"symbol": s, "qty": str(q)} for s, q in context["positions"].items()
            ]
        if path == "v2/stocks/quotes/latest" and method == "GET":
            symbols = request.query_params["symbols"].split(",")
            return {
                "quotes": {
                    s: {
                        "ap": context["quotes"][s],
                        "bp": context["quotes"][s],
                        "t": "2026-01-01T00:00:00Z",
                    }
                    for s in symbols
                }
            }
        if path == "v2/orders" and method == "POST":
            if faults.get("reject"):
                raise HTTPException(422, "Injected rejection")

            cid = body["client_order_id"]
            if cid in context["orders"]:
                raise HTTPException(422, "Duplicate client order ID")

            status = faults.get("order_status", "filled")
            order = {
                **body,
                "id": str(uuid4()),
                "status": status,
                "filled_qty": body["qty"] if status == "filled" else "0",
            }
            context["orders"][cid] = order

            if status == "filled":
                fill(context, order, float(body["qty"]))
            elif status == "partially_filled":
                order["filled_qty"] = str(float(body["qty"]) / 2)
                fill(context, order, float(order["filled_qty"]))
            if faults.get("disconnect_after_acceptance"):
                return Response("", status_code=503)
            if faults.get("submission_delay"):
                await asyncio.sleep(faults["submission_delay"])

            return order

        if path == "v2/orders" and method == "GET":
            return [
                o
                for o in context["orders"].values()
                if request.query_params.get("status") != "open"
                or o["status"] not in ("filled", "canceled", "rejected", "expired")
            ]

        if path == "v2/orders:by_client_order_id" and method == "GET":
            order = context["orders"].get(request.query_params["client_order_id"])
        elif path.startswith("v2/orders/"):
            order = next(
                (
                    o
                    for o in context["orders"].values()
                    if o["id"] == path.split("/")[-1]
                ),
                None,
            )
        else:
            raise HTTPException(404, "Endpoint outside mock surface")
        if not order:
            raise HTTPException(404, "Order not found")
        if method == "DELETE":
            if faults.get("cancel_failure"):
                raise HTTPException(503, "Cancellation failed")
            if faults.get("fill_on_cancel") and order["status"] not in (
                "filled",
                "canceled",
            ):
                remaining = float(order["qty"]) - float(order["filled_qty"])
                fill(context, order, remaining)
                order.update(status="filled", filled_qty=order["qty"])
            elif order["status"] != "filled":
                order["status"] = "canceled"
            return Response(status_code=204)

        return order

    return app


def fill(context, order, quantity):
    symbol = order["symbol"]
    signed = quantity if order["side"] == "buy" else -quantity
    context["positions"][symbol] = context["positions"].get(symbol, 0) + signed


app = create_app()
