from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.config import settings
from api.router import api_router
from kafka.producer import init_kafka_producer, close_kafka_producer
from db.redis_client import init_redis_client, close_redis_client
from websocket.router import router as websocket_router
from websocket.manager import manager as websocket_manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_kafka_producer()
    await init_redis_client()
    await websocket_manager.start_redis_listener()
    yield
    # Shutdown
    await websocket_manager.stop_redis_listener()
    await close_redis_client()
    await close_kafka_producer()

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

# CORS (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)
# Include WebSocket router
app.include_router(websocket_router, prefix="/ws", tags=["websocket"])

@app.get("/")
async def root():
    return {"message": "Welcome to DivyaPath Real-time Tracking System"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
