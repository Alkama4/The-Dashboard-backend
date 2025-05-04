# Standard libraries
import asyncio
import redis.asyncio as redis
import os
import mysql.connector
import httpx
from fastapi import HTTPException
from datetime import timedelta
import json
import aiomysql
from contextlib import asynccontextmanager

# Semaphore so that we don't overwhelm the network with hundreads of conncections.
semaphore = asyncio.Semaphore(5)

# Set up aioredis client
redis_client = redis.from_url(os.getenv("REDIS_PATH", "redis://127.0.0.1:6379"), decode_responses=True)


# Establishes an asynchronous connection to the MySQL database
# Do not use this to connect, instead use the "aiomysql_conn_get" to use as the connection
async def aiomysql_connect():
    return await aiomysql.connect(
        user=os.getenv("DB_USER", "default"),
        password=os.getenv("DB_PASSWORD", "default"),
        db=os.getenv("DB_NAME", "default"),
        host=os.getenv("DB_HOST", "default"),
        port=3306
    )


# Used in "async with aiomysql_conn_get() as conn:" to avoid having to always close the conn
@asynccontextmanager
async def aiomysql_conn_get():
    conn = await aiomysql_connect()
    try:
        yield conn
    finally:
        conn.close()


# Execute a MySQL query and return result
async def aiomysql_conn_execute(
    conn,
    query: str,
    params: tuple = (),
    use_dictionary: bool = True,
    return_lastrowid: bool = False,
    return_rowcount: bool = False
) -> list:
    # Select cursor class (dictionary or normal)
    cursor_class = aiomysql.DictCursor if use_dictionary else aiomysql.Cursor

    async with conn.cursor(cursor_class) as cursor:
        # Execute query with parameters
        await cursor.execute(query, params)

        # Commit if query modifies data
        if query.strip().lower().startswith(("insert", "update", "delete")):
            await conn.commit()

        # Return based on flags
        if return_lastrowid:
            return cursor.lastrowid
        if return_rowcount:
            return cursor.rowcount
        return await cursor.fetchall()


# Executes a single MySQL query and returns the result as a list of dictionaries.
# Suitable for single queries. For multiple queries, use the `conn` method directly.
async def aiomysql_execute(query: str, params: tuple = ()) -> list:
    conn = await aiomysql_connect()
    try:
        return await aiomysql_conn_execute(conn, query, params)
    except aiomysql.MySQLError as e:
        detail = f"MySQL error: {e}"
        print(detail)
        raise HTTPException(status_code=500, detail=detail)
    except Exception as e:
        detail = f"Unknown error with aiomysql: {e}"
        print(detail)
        raise HTTPException(status_code=500, detail=detail)
    finally:
        conn.close()


# Helper function to store to redis cache
async def add_to_cache(key: str, data: dict, timedelta: timedelta):
    # Store the data as JSON in Redis with an expiration of 1 week
    await redis_client.setex(key, timedelta, json.dumps(data))


# Helper function to retrieve from redis cache
async def get_from_cache(key: str) -> dict:
    # Retrieve data from Redis and parse it
    data = await redis_client.get(key)
    if data:
        return json.loads(data)
    return None


# Function to query the TMDB servers
async def query_tmdb(endpoint: str, params: dict = {}):
    headers = {
        "Authorization": f"Bearer {os.getenv('TMDB_ACCESS_TOKEN', 'default_token')}",
        "Accept": "application/json"
    }
    params["language"] = "en-US"

    print(f"Querying TMDB: {endpoint}")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://api.themoviedb.org/3{endpoint}", params=params, headers=headers)
        return response.json() if response.status_code == 200 else {}


# Function to query for additional data like IMDB ratings from OMDB
async def query_omdb(imdb_id: str):
    params = {}
    params["apikey"] = os.getenv('OMDB_APIKEY', 'default_key')
    params["i"] = imdb_id

    print(f"Querying OMDB: {imdb_id}")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://www.omdbapi.com", params=params)
        return response.json() if response.status_code == 200 else {}


# Download an image from an url, semaphore to limit the amount of async tasks.
async def download_image(image_url: str, image_save_path: str, replace = False):
    try:
        # Skip download if file already exists and the replace flag isn't set to true
        if os.path.exists(image_save_path) and not replace:
            print(f"Image already exists: {image_save_path}, skipping download.")
            return

        global semaphore
        async with semaphore:
            async with httpx.AsyncClient(timeout=None) as client:
                response = await client.get(image_url)

        if response.status_code == 200:
            # Saving part is unlimited, no semaphore needed here
            with open(image_save_path, 'wb') as f:
                f.write(response.content)
            print(f"Image saved at {image_save_path}")
        else:
            raise HTTPException(status_code=response.status_code, detail="Failed to download image")

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail="Failed to fetch image")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Used to validate the sesion key
async def validate_session_key_conn(conn, session_key=None, guest_lock=True):
    if session_key != None and session_key != '' and session_key != 'null':

        # Validate the session and fetch user_id
        session_query = "SELECT user_id FROM sessions WHERE session_id = %s AND expires_at > NOW()"
        session_result = await aiomysql_conn_execute(conn, session_query, (session_key,), use_dictionary=False)

        if not session_result:
            raise HTTPException(status_code=403, detail="Invalid or expired session key.")
        
        return session_result[0][0]
    
    elif not guest_lock:
        return 1  # Default to guest's user_id (1) if no session key is provided
    
    else:
        raise HTTPException(status_code=405, detail="Account required.")


# Used to get settings values e.g. for title limit
async def fetch_user_settings(conn, user_id: int, setting_name: str):
    query = f"SELECT {setting_name} FROM user_settings WHERE user_id = %s"
    result = await aiomysql_conn_execute(conn, query, (user_id,), use_dictionary=True)
    return result[0][setting_name] if result else None


# Used to format a time difference, created for backups
def format_time_difference(delta):
    days = delta.days
    seconds = delta.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    time_parts = []
    if days > 0:
        time_parts.append(f"{days}pv")
    if hours > 0:
        time_parts.append(f"{hours}h")
    if minutes > 0:
        time_parts.append(f"{minutes}min")
    if seconds > 0:
        time_parts.append(f"{seconds}s")

    return " ".join(time_parts[:2])
