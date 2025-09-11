# External imports
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import json


# Internal imports
from routers.account import router as account_router 
from routers.media import router as media_router
from routers.server import router as server_router 
from routers.spendings import router as spendings_router 
from routers.watch_list import router as watch_list_router 
from utils import redis_client

# Create fastAPI instance and set CORS middleware
# Could limit the addresses but works fine as is, since only hosted on LAN.
app = FastAPI(root_path="/api")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # List of allowed origins
    allow_methods=["*"],      # List of allowed HTTP methods (GET, POST, etc.)
    allow_headers=["*"],      # List of allowed headers
)

# Include the account routes in the FastAPI app
app.include_router(account_router, prefix="/account", tags=["account"])
app.include_router(media_router, prefix="/media", tags=["media"])
app.include_router(server_router, prefix="/server", tags=["server"])
app.include_router(spendings_router, prefix="/spendings", tags=["spendings"])
app.include_router(watch_list_router, prefix="/watch_list", tags=["watch_list"])


# Runs everytime an endpoint is called. Used to log request for analysis.
@app.middleware("http")
async def log_request_data(request: Request, call_next):
    if request.url.path == "/api/server/logs/system_resources":
        return await call_next(request)

    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    client_ip = request.headers.get("x-forwarded-for", request.client.host).split(",")[0].strip()

    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "endpoint": request.url.path,
        "status_code": response.status_code,
        "backend_time_ms": round(process_time * 1000, 2),
        "client_ip": client_ip,
        "method": request.method
    }

    MAX_LOGS_AMOUNT = 10000

    await redis_client.lpush("fastapi_request_logs", json.dumps(log_entry))
    await redis_client.ltrim("fastapi_request_logs", 0, MAX_LOGS_AMOUNT - 1)

    return response


# Landing page that dynamically shows endpoints
@app.get("/")
def root(request: Request):
    endpoints = [
        f"{', '.join(route.methods)} {route.path}"
        for route in app.routes
        if route.methods
    ]

    return {
        "Head": "Hello world!",
        "Text": "Welcome to the FastAPI backend for the Vue.js frontend. The available endpoints are listed below.",
        "Request came from": request.client.host,
        "Endpoints": endpoints
    }

