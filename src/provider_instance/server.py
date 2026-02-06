from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.provider_instance.routes import router
from src.provider_instance import engine_instance, setup_logging

app = FastAPI(
    title="Provider Service API",
    description="API for accessing Engine and providers",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.on_event("startup")
async def startup():
    setup_logging()
    await engine_instance.initialize()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)