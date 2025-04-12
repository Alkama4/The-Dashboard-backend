# Standard libraries
import asyncio
import os
from collections import OrderedDict
import mysql.connector
import httpx
from fastapi import HTTPException

# Keep a couple copies of the search results cached for the duration of the server runtime
# Might want to replace this with a mysql table wit BLOBs in the future because of workers.
tmdbQueryCacheMaxSize = 5
tmdbQueryCache = OrderedDict()

# Semaphore so that we don't overwhelm the network with hundreads of conncections.
semaphore = asyncio.Semaphore(5)


# Function to connect to MySQL and perform a query
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


# Add search queries to cache since it takes so long to fetch them
def add_to_cache(key, value):
    global tmdbQueryCache
    
    # If the cache is full, pop the oldest item
    if len(tmdbQueryCache) >= tmdbQueryCacheMaxSize:
        tmdbQueryCache.popitem(last=False)  # Remove the oldest item
    
    # Add the new item to the cache
    tmdbQueryCache[key] = value
