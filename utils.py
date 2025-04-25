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

# Semaphore so that we don't overwhelm the network with hundreads of conncections.
semaphore = asyncio.Semaphore(5)

# Set up aioredis client
redis_client = redis.from_url(os.getenv("REDIS_PATH", "redis://127.0.0.1:6379"), decode_responses=True)


# Establishes an asynchronous connection to the MySQL database
async def aiomysql_connect():
    return await aiomysql.connect(
        user=os.getenv("DB_USER", "default"),
        password=os.getenv("DB_PASSWORD", "default"),
        db=os.getenv("DB_NAME", "default"),
        host=os.getenv("DB_HOST", "default"),
        port=3306
    )

# Executes a MySQL query asynchronously and returns the result as a list of dictionaries
async def aiomysql_conn_execute(conn, query: str, params: tuple = ()) -> list:
    async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(query, params)
        result = await cursor.fetchall()
        return result

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


# Old sync (not async) function to connect to MySQL and perform a query
def query_mysql(query: str, params: tuple = (), fetch_last_row_id=False, use_dictionary=False):
    try:
        # Test to see if it can communicate on the bridge network and if that makes a difference
        # mysql_host = "172.18.0.3"
        # Doesn't really and only adds a point of failure when the ip changes. Just use the external ip provided by env.
        mysql_host = os.getenv("DB_HOST", "default")
        mysql_user = os.getenv("DB_USER", "default")
        mysql_password = os.getenv("DB_PASSWORD", "default")
        mysql_db = os.getenv("DB_NAME", "default")

        # Create a synchronous connection
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db,
        )

        if conn is None:
            raise HTTPException(status_code=500, detail="Failed to establish database connection.")

        cursor = conn.cursor(dictionary=use_dictionary)

        # Execute the query
        cursor.execute(query, params)

        # Retrieve last inserted ID if requested
        if fetch_last_row_id:
            lastrowid = cursor.lastrowid
            conn.commit()
            cursor.close()
            conn.close()
            return lastrowid

        # If the query is a SELECT, fetch the results
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result

        # Return affected rows for DELETE, UPDATE, INSERT
        affected_rows = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return affected_rows

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL query error: {str(e)}")


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
def query_tmdb(endpoint: str, params: dict = {}):
    headers = {
        "Authorization": f"Bearer {os.getenv('TMDB_ACCESS_TOKEN', 'default_token')}",
        "Accept": "application/json"
    }
    params["language"] = "en-US"

    print(f"Querying TMDB: {endpoint}")
    
    with httpx.Client() as client:
        response = client.get(f"https://api.themoviedb.org/3{endpoint}", params=params, headers=headers)
        return response.json() if response.status_code == 200 else {}


# Function to query for additional data like IMDB ratings from OMDB
def query_omdb(imdb_id: str):
    params = {}
    params["apikey"] = os.getenv('OMDB_APIKEY', 'default_key')
    params["i"] = imdb_id

    print(f"Querying OMDB: {imdb_id}")
    
    with httpx.Client() as client:
        response = client.get(f"https://www.omdbapi.com", params=params)
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
def validate_session_key(session_key=None, guest_lock=True):
    if session_key != None and session_key != '' and session_key != 'null':
        # Validate the session and fetch user_id
        session_query = "SELECT user_id FROM sessions WHERE session_id = %s AND expires_at > NOW()"
        session_result = query_mysql(session_query, (session_key,))
        if not session_result:
            raise HTTPException(status_code=403, detail="Invalid or expired session key.")
        return session_result[0][0]
    elif not guest_lock:
        return 1  # Default to guest's user_id (1) if no session key is provided
    else:
        raise HTTPException(status_code=405, detail="Account required.")


# Used to get settings values e.g. for title limit
def fetch_user_settings(user_id: int, setting_name: str):
    query = f"SELECT {setting_name} FROM user_settings WHERE user_id = %s"
    result = query_mysql(query, (user_id,), use_dictionary=True)
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
