# External imports
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware


# Internal imports
from app.routers.account import router as account_router 
from app.routers.media import router as media_router
from app.routers.spendings import router as spendings_router 
from app.routers.watch_list import router as watch_list_router 

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
app.include_router(spendings_router, prefix="/spendings", tags=["spendings"])
app.include_router(watch_list_router, prefix="/watch_list", tags=["watch_list"])


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

