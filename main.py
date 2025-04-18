# External imports
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# Internal imports
from routers.account import router as account_router 
from routers.media import router as media_router
from routers.redirect import router as redirect_router 
from routers.server import router as server_router 
from routers.spendings import router as spendings_router 
from routers.watch_list import router as watch_list_router 
from utils import query_mysql

# Create fastAPI instance and set CORS middleware
# Could limit to only my local addresses but works fine as is.
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # List of allowed origins
    allow_methods=["*"],      # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],      # Allow all headers
)

# Include the account routes in the FastAPI app
app.include_router(account_router, prefix="/account", tags=["account"])
app.include_router(media_router, prefix="/media", tags=["media"])
app.include_router(redirect_router, prefix="/redirect", tags=["redirect"])
app.include_router(server_router, prefix="/server", tags=["server"])
app.include_router(spendings_router, prefix="/spendings", tags=["spendings"])
app.include_router(watch_list_router, prefix="/watch_list", tags=["watch_list"])

# - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - BASIC TOOLS - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - #

# Runs everytime an endpoint is called
# Used to log request for analysis
@app.middleware("http")
async def log_request_data(request: Request, call_next):

    # Skip if the automated server log call
    if request.url.path == "/store_server_resource_logs":
        return await call_next(request)

    # Get values for the log
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    endpoint = request.url.path
    status_code = response.status_code
    client_ip = request.client.host
    method = request.method

    # Push the data to mysql
    insert_query = """
    INSERT INTO server_fastapi_request_logs (endpoint, status_code, backend_time_ms, client_ip, method)
    VALUES (%s, %s, %s, %s, %s)
    """
    query_mysql(insert_query, (endpoint, status_code, process_time * 1000, client_ip, method))

    return response


# Landing page that shows dynamically generated endpoints
@app.get("/")
def root(request: Request):
    # Dynamically generate a list of all available endpoints
    endpoints = [route.path for route in app.routes]
    
    return {
        "Head": "Hello world!",
        "Text": "Welcome to the FastAPI backend for the Vue.js frontend. The available endpoints are listed below.",
        "Request came from": request.client.host,
        "Endpoints": endpoints
    }