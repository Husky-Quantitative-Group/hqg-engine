from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse


def create_app():
    app = FastAPI()
    faults = {"control": False, "website": False, "generic_health": False}
    app.state.faults = faults

    @app.post("/test/faults")
    async def change(request: Request):
        faults.update(await request.json())
        return faults

    @app.get("/control")
    async def control():
        return PlainTextResponse(
            "healthy" if faults["generic_health"] else "hqg-account-control-ready",
            status_code=503 if faults["control"] else 200,
        )

    @app.get("/")
    async def website():
        return HTMLResponse(
            '<html><main id="hqg-dashboard-app">HQG Dashboard</main></html>',
            status_code=503 if faults["website"] else 200,
        )

    return app


app = create_app()
