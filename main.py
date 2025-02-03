from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from datetime import datetime, timedelta, date
import random
import string
import calendar
import os
import psutil
from croniter import croniter
from zoneinfo import ZoneInfo
from collections import defaultdict
import pandas as pd
import httpx

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # List of allowed origins  
    allow_methods=["*"],      # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],      # Allow all headers
)

# - - - - - - - - - - - - - -  - - - - - - - #
# - - - - - - - ON APP STARTUP - - - - - - - #
# - - - - - - - - - - - - - -  - - - - - - - #

# Temporary values that are only used while testing and developing
tempCachedSearchResults = {
  "page": 1,
  "results": [
    {
      "adult": False,
      "backdrop_path": "/eD7FnB7LLrBV5ewjdGLYTAoV9Mv.jpg",
      "genre_ids": [
        28,
        53
      ],
      "id": 245891,
      "original_language": "en",
      "original_title": "John Wick",
      "overview": "Ex-hitman John Wick comes out of retirement to track down the gangsters that took everything from him.",
      "popularity": 67.286,
      "poster_path": "/fZPSd91yGE9fCcCe6OoQr6E3Bev.jpg",
      "release_date": "2014-10-22",
      "title": "John Wick",
      "video": False,
      "vote_average": 7.441,
      "vote_count": 19412,
      "added": False
    },
    {
      "adult": False,
      "backdrop_path": "/7I6VUdPj6tQECNHdviJkUHD2u89.jpg",
      "genre_ids": [
        28,
        53,
        80
      ],
      "id": 603692,
      "original_language": "en",
      "original_title": "John Wick: Chapter 4",
      "overview": "With the price on his head ever increasing, John Wick uncovers a path to defeating The High Table. But before he can earn his freedom, Wick must face off against a new enemy with powerful alliances across the globe and forces that turn old friends into foes.",
      "popularity": 157.406,
      "poster_path": "/vZloFAK7NmvMGKE7VkF5UHaz0I.jpg",
      "release_date": "2023-03-22",
      "title": "John Wick: Chapter 4",
      "video": False,
      "vote_average": 7.7,
      "vote_count": 6800,
      "added": False
    },
    {
      "adult": False,
      "backdrop_path": "/r17jFHAemzcWPPtoO0UxjIX0xas.jpg",
      "genre_ids": [
        28,
        53,
        80
      ],
      "id": 324552,
      "original_language": "en",
      "original_title": "John Wick: Chapter 2",
      "overview": "John Wick is forced out of retirement by a former associate looking to seize control of a shadowy international assassins’ guild. Bound by a blood oath to aid him, Wick travels to Rome and does battle against some of the world’s most dangerous killers.",
      "popularity": 87.215,
      "poster_path": "/hXWBc0ioZP3cN4zCu6SN3YHXZVO.jpg",
      "release_date": "2017-02-08",
      "title": "John Wick: Chapter 2",
      "video": False,
      "vote_average": 7.3,
      "vote_count": 13196,
      "added": True
    },
    {
      "adult": False,
      "backdrop_path": "/vVpEOvdxVBP2aV166j5Xlvb5Cdc.jpg",
      "genre_ids": [
        28,
        53,
        80
      ],
      "id": 458156,
      "original_language": "en",
      "original_title": "John Wick: Chapter 3 - Parabellum",
      "overview": "Super-assassin John Wick returns with a $14 million price tag on his head and an army of bounty-hunting killers on his trail. After killing a member of the shadowy international assassin’s guild, the High Table, John Wick is excommunicado, but the world’s most ruthless hit men and women await his every turn.",
      "popularity": 78.205,
      "poster_path": "/ziEuG1essDuWuC5lpWUaw1uXY2O.jpg",
      "release_date": "2019-05-15",
      "title": "John Wick: Chapter 3 - Parabellum",
      "video": False,
      "vote_average": 7.4,
      "vote_count": 10765
    },
    {
      "adult": False,
      "backdrop_path": "/5IbxcBXZ5EhVAttfNDMPncikudr.jpg",
      "genre_ids": [
        28,
        53,
        80
      ],
      "id": 541671,
      "original_language": "en",
      "original_title": "From the World of John Wick: Ballerina",
      "overview": "Taking place during the events of John Wick: Chapter 3 - Parabellum, Eve Macarro begins her training in the assassin traditions of the Ruska Roma.",
      "popularity": 37.998,
      "poster_path": "/bN9431goQjR5Lu0VziD7iKW0Hfd.jpg",
      "release_date": "2025-06-04",
      "title": "From the World of John Wick: Ballerina",
      "video": False,
      "vote_average": 0,
      "vote_count": 0
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [
        28,
        80,
        53
      ],
      "id": 730629,
      "original_language": "en",
      "original_title": "John Wick: Chapter 5",
      "overview": "The fifth installment in the John Wick franchise. Plot TBA.",
      "popularity": 15.064,
      "poster_path": None,
      "release_date": "",
      "title": "John Wick: Chapter 5",
      "video": False,
      "vote_average": 0,
      "vote_count": 0
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [],
      "id": 619022,
      "original_language": "en",
      "original_title": "John Wick: Assassin's Code",
      "overview": "John Wick Movie Extra",
      "popularity": 11.962,
      "poster_path": "/fJbw16AwM59dEhSiCIAfFGgIgOP.jpg",
      "release_date": "2015-02-03",
      "title": "John Wick: Assassin's Code",
      "video": True,
      "vote_average": 7.704,
      "vote_count": 49
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [
        99
      ],
      "id": 600991,
      "original_language": "en",
      "original_title": "Training 'John Wick'",
      "overview": "A look at the fight choreography being developed for the film.",
      "popularity": 6.569,
      "poster_path": "/1x1fEoDe3GKBYh4iR4jhrouRXzT.jpg",
      "release_date": "2017-06-13",
      "title": "Training 'John Wick'",
      "video": True,
      "vote_average": 7.381,
      "vote_count": 42
    },
    {
      "adult": False,
      "backdrop_path": "/vWNGnjBB3pa6R8slwjhwDxRqBUf.jpg",
      "genre_ids": [
        99
      ],
      "id": 651445,
      "original_language": "en",
      "original_title": "John Wick Chapter 2: Wick-vizzed",
      "overview": "A candid look at rehearsal footage in support of a focus on pre-viz.",
      "popularity": 6.78,
      "poster_path": "/qQFBj2tBlkKhAcAgDVpdFWLX5x.jpg",
      "release_date": "2017-06-13",
      "title": "John Wick Chapter 2: Wick-vizzed",
      "video": True,
      "vote_average": 7.519,
      "vote_count": 80
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [
        99
      ],
      "id": 600987,
      "original_language": "en",
      "original_title": "Don't F*#% With John Wick",
      "overview": "Behind the scenes look at fight choreography and action training.",
      "popularity": 3.871,
      "poster_path": "/d3m7SxiehljQ2r5dIHg7fGjfLXp.jpg",
      "release_date": "2015-02-03",
      "title": "Don't F*#% With John Wick",
      "video": False,
      "vote_average": 7.522,
      "vote_count": 45
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [
        99
      ],
      "id": 600990,
      "original_language": "en",
      "original_title": "John Wick: Calling in the Cavalry",
      "overview": "Short documentary that looks at a number of elements like the initial pitch for the project and the 2nd Unit action sequences.",
      "popularity": 2.233,
      "poster_path": "/gps49Xqjv0C6Kplb9jgDVAaE9CF.jpg",
      "release_date": "2015-02-03",
      "title": "John Wick: Calling in the Cavalry",
      "video": False,
      "vote_average": 7.194,
      "vote_count": 18
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [
        99
      ],
      "id": 1036194,
      "original_language": "en",
      "original_title": "As Above, So Below: The Underworld of 'John Wick'",
      "overview": "A close look at the assassin's lifestyle in the film.",
      "popularity": 4.005,
      "poster_path": None,
      "release_date": "2017-06-13",
      "title": "As Above, So Below: The Underworld of 'John Wick'",
      "video": True,
      "vote_average": 7.535,
      "vote_count": 43
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [
        28,
        80,
        53
      ],
      "id": 1290912,
      "original_language": "en",
      "original_title": "From the World of John Wick: Caine",
      "overview": "A spin-off to John Wick: Chapter 4 (2023), focusing on Donnie Yen's character Caine after he has been freed from his obligations to the High Table.",
      "popularity": 6.445,
      "poster_path": "/kqzN4QkrOEmtNHO6SejeoKqT7aW.jpg",
      "release_date": "",
      "title": "From the World of John Wick: Caine",
      "video": False,
      "vote_average": 0,
      "vote_count": 0
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [],
      "id": 1412587,
      "original_language": "en",
      "original_title": "John Wick: Kill Count",
      "overview": "This piece recaps all of the kills in the film.",
      "popularity": 1.302,
      "poster_path": "/85UbK3kmRQgOXiTul4Ux15Ony4d.jpg",
      "release_date": "2017-06-13",
      "title": "John Wick: Kill Count",
      "video": False,
      "vote_average": 7,
      "vote_count": 1
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [],
      "id": 1412091,
      "original_language": "en",
      "original_title": "John Wick: NYC Noir",
      "overview": "There is currently no summary",
      "popularity": 1.729,
      "poster_path": "/wOy0Y9AygYgUFV28dNcQt1bloZh.jpg",
      "release_date": "2015-02-03",
      "title": "John Wick: NYC Noir",
      "video": False,
      "vote_average": 7,
      "vote_count": 1
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [],
      "id": 1412096,
      "original_language": "en",
      "original_title": "John Wick: The Red Circle",
      "overview": "John Wick movie extras",
      "popularity": 0.218,
      "poster_path": "/wazHDUfdTQVkgYYBQp3aFAPI7Pk.jpg",
      "release_date": "2015-02-03",
      "title": "John Wick: The Red Circle",
      "video": False,
      "vote_average": 0,
      "vote_count": 0
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [],
      "id": 1412104,
      "original_language": "en",
      "original_title": "John Wick: Car Fu Ride-Along",
      "overview": "A look at the several Mustangs they used, which perhaps gives vintage car lovers hopes that some of them at least escaped without a scratch.",
      "popularity": 0.363,
      "poster_path": "/jIetn6jz6x0oLnBlGBe2KPkPDqN.jpg",
      "release_date": "2017-06-13",
      "title": "John Wick: Car Fu Ride-Along",
      "video": False,
      "vote_average": 7,
      "vote_count": 1
    },
    {
      "adult": False,
      "backdrop_path": None,
      "genre_ids": [],
      "id": 1412106,
      "original_language": "en",
      "original_title": "Retro Wick: Exploring the Unexpected Success of 'John Wick'",
      "overview": "A look at the phenomenal excitement the first film generated.",
      "popularity": 0.769,
      "poster_path": "/qF9kAHUt8ewbO8YX2OF00EQmjYL.jpg",
      "release_date": "2017-06-13",
      "title": "Retro Wick: Exploring the Unexpected Success of 'John Wick'",
      "video": False,
      "vote_average": 7,
      "vote_count": 2
    }
  ],
  "total_pages": 1,
  "total_results": 18
}



# - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - BASIC TOOLS - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - #

# Function to connect to MySQL and perform a query
def query_mysql(query: str, params: tuple = (), fetch_lastrowid=False):
    try:
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

        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query, params)

        # Retrieve last inserted ID if requested
        if fetch_lastrowid:
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

        # Handle insert or other queries without return value
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL query error: {str(e)}")


# Function to query the TMDB servers
def query_tmdb(endpoint: str, params: dict = {}):
    params["api_key"] = os.getenv("TMDB_API_KEY", "default value if not found")
    params["language"] = "en-US"
    print(f"Querying TMDB: {endpoint}")
    with httpx.Client() as client:
        response = client.get(f"https://api.themoviedb.org/3{endpoint}", params=params)
        return response.json() if response.status_code == 200 else {}


def validateSessionKey(session_key=None, guest_lock=True):
    if session_key != None and session_key != '':
        # Validate the session and fetch userID
        session_query = "SELECT userID FROM sessions WHERE sessionID = %s AND expires_at > NOW()"
        session_result = query_mysql(session_query, (session_key,))
        if not session_result:
            raise HTTPException(status_code=403, detail="Invalid or expired session key.")
        return session_result[0][0]
    elif not guest_lock:
        return 1  # Default to guest's userID (1) if no session key is provided
    else:
        raise HTTPException(status_code=405, detail="Account required.")


@app.get("/")
def root():
    return {
        "Head": "Hello world!",
        "Text": "Welcome to the FastAPI backend for the Vue.js frontend. The available endpoints are listed below.",
        "Endp": [
            "/  (This page)",
            "/login",
            "/get_login_status",
            "/logout",
            "/new_transaction",
            "/get_transactions",
            "/get_options",
            "/get_filters"
        ]
    }


# - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - GENERAL LOGINS ETC. - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

@app.post("/login")
def login(
    username: str = Query(...),
    password: str = Query(...),
    previousSessionKey: str = Query(None)
):
    # Check if the user is already logged in
    if previousSessionKey:
        # Check if the session key exists and join it with the user table to get the username
        query = """
            SELECT username 
            FROM users 
            WHERE userID = (
                SELECT userID 
                FROM sessions 
                WHERE sessionID = %s AND expires_at > NOW()
            );
        """
        result = query_mysql(query, (previousSessionKey,))
        if result:
            logged_in_username = result[0][0]  # Extract the username from the query result
            if logged_in_username.lower() == username.lower():
                return {
                    "loginStatus": "warning", 
                    "statusMessage": "Already logged in.",
                    "sessionKey": previousSessionKey, 
                }

    # Check if the user exists in the database and query the password
    query = "SELECT userID, password FROM users WHERE username = %s"
    user = query_mysql(query, (username,))
    
    # Basic password check (plaintext)
    if not user or user[0][1] != password:  
        return {
            "loginStatus": "error",
            "statusMessage": "Invalid username or password."
            }   

    # Generate a session key
    session_key = ''.join(random.choices(string.ascii_letters + string.digits, k=36))

    # Set the session expiration time
    expiration_time = datetime.now() + timedelta(days=14)

    # Insert the session key into the sessions table
    user_id = user[0][0]  # Get userID from the query result
    insert_query = """
        INSERT INTO sessions (sessionID, userID, expires_at) 
        VALUES (%s, %s, %s)
    """
    query_mysql(insert_query, (session_key, user_id, expiration_time))

    # Lastly delete expired sessions from the sessions table
    expired_query = """
        DELETE FROM sessions WHERE expires_at <= NOW();
    """
    query_mysql(expired_query)

    # Return the session key to the client
    return {
        "loginStatus": "success",
        "sessionKey": session_key,
        "username": username,
    }


@app.post("/get_login_status")
def get_login_status(
    sessionKey: str = Query(...)
):
    # Check if the session key exists and join it with the user table to get the username
    query = """
    SELECT username 
    FROM users 
    WHERE userID = (
        SELECT userID 
        FROM sessions 
        WHERE sessionID = %s AND expires_at > NOW()
    );
    """
    result = query_mysql(query, (sessionKey,))
    if result:
        return {
            "loggedIn": True, 
            "username": result[0][0],
        }
    else:
        return {
            "loggedIn": False,
        }
    

@app.post("/logout")
def logout(
    sessionKey: str = Query(...)
):
    # Delete the session key from the sessions table
    query = "DELETE FROM sessions WHERE sessionID = %s"
    query_mysql(query, (sessionKey,))
    return {
        "logOutSuccess": True,
    }


# - - - - - - - - - - - - -  - - - - - - - #
# - - - - - - - TRANSACTIONS - - - - - - - #
# - - - - - - - - - - - - -  - - - - - - - #
@app.post("/new_transaction")
def new_transaction(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, True)

        # Extract transaction data
        direction = data.get("direction")
        date = data.get("date")
        counterparty = data.get("counterparty")
        notes = data.get("notes")
        categories = data.get("categories", [])

        # Validate required fields
        if not all([direction, date, counterparty, categories]):
            raise HTTPException(status_code=400, detail="Missing required transaction fields.")

        # Insert the transaction into the transactions table
        transaction_query = (
            "INSERT INTO transactions (direction, date, counterparty, notes, userID) "
            "VALUES (%s, %s, %s, %s, %s)"
        )
        transaction_id = query_mysql(transaction_query, (direction, date, counterparty, notes, user_id), fetch_lastrowid=True)

        if not transaction_id:
            raise HTTPException(status_code=500, detail="Failed to retrieve transaction ID.")

        # Insert each category into the transaction_items table
        for category in categories:
            category_name = category.get("category")
            amount = category.get("amount")

            if not all([category_name, amount]):
                raise HTTPException(status_code=400, detail="Category and amount are required for each item.")

            item_query = (
                "INSERT INTO transaction_items (transactionID, category, amount) "
                "VALUES (%s, %s, %s)"
            )
            query_mysql(item_query, (transaction_id, category_name, amount))

        return {"newTransactionSuccess": True}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/edit_transaction")
def edit_transaction(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, True)

        # Extract transaction data
        transaction_id = data.get("transactionID")
        direction = data.get("direction")
        date = data.get("date")
        counterparty = data.get("counterparty")
        notes = data.get("notes")
        categories = data.get("categories", [])
        
        print(date)

        # Validate required fields
        if not all([transaction_id, direction, date, counterparty, categories]):
            raise HTTPException(status_code=400, detail="Missing required transaction fields.")

        # Check if the transaction exists and belongs to the user
        transaction_query = "SELECT * FROM transactions WHERE transactionID = %s AND userID = %s"
        transaction_result = query_mysql(transaction_query, (transaction_id, user_id))
        if not transaction_result:
            raise HTTPException(status_code=403, detail="Transaction not found or not owned by the user.")

        # Update the transaction in the transactions table
        update_transaction_query = (
            "UPDATE transactions SET direction = %s, date = %s, counterparty = %s, notes = %s "
            "WHERE transactionID = %s AND userID = %s"
        )
        query_mysql(update_transaction_query, (direction, date, counterparty, notes, transaction_id, user_id))

        # Delete existing transaction items
        delete_items_query = "DELETE FROM transaction_items WHERE transactionID = %s"
        query_mysql(delete_items_query, (transaction_id,))

        # Insert new categories into the transaction_items table
        for category in categories:
            category_name = category.get("category")
            amount = category.get("amount")

            if not all([category_name, amount]):
                raise HTTPException(status_code=400, detail="Category and amount are required for each item.")

            item_query = (
                "INSERT INTO transaction_items (transactionID, category, amount) "
                "VALUES (%s, %s, %s)"
            )
            query_mysql(item_query, (transaction_id, category_name, amount))

        return {"editTransactionSuccess": True}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/delete_transaction")
def delete_transaction(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, True)

        # Extract transaction data
        transaction_id = data.get("transactionID")

        # Validate required fields
        if not transaction_id:
            raise HTTPException(status_code=400, detail="Transaction ID is required.")

        # Check if the transaction exists and belongs to the user
        transaction_query = "SELECT * FROM transactions WHERE transactionID = %s AND userID = %s"
        transaction_result = query_mysql(transaction_query, (transaction_id, user_id))
        if not transaction_result:
            raise HTTPException(status_code=403, detail="Transaction not found or not owned by the user.")

        # Delete associated transaction items
        delete_items_query = "DELETE FROM transaction_items WHERE transactionID = %s"
        query_mysql(delete_items_query, (transaction_id,))

        # Delete the transaction
        delete_transaction_query = "DELETE FROM transactions WHERE transactionID = %s AND userID = %s"
        query_mysql(delete_transaction_query, (transaction_id, user_id))

        return {"deleteTransactionSuccess": True}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/get_transactions")
def get_transactions(
    sort_by: str = Query("date", regex="^(date|counterparty|category|amount|notes)$"),
    sort_order: str = Query("asc", regex="^(asc|desc)$"),
    start_date: str = Query(None),
    end_date: str = Query(None),
    min_amount: float = Query(None),
    max_amount: float = Query(None),
    counterparties: str = Query(None),
    counterparty_inclusion_mode: bool = Query(True),
    categories: str = Query(None),
    category_inclusion_mode: bool = Query(True),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
    session_key: str = Query(None)
):
    # Validate the session key
    userID = validateSessionKey(session_key, False)

    # Initialize filters and parameters
    filters = ["t.userID = %s"]
    params = [userID]

    # Use the appropriate timezone (e.g., Europe/Helsinki)
    local_timezone = ZoneInfo("Europe/Helsinki")

    if start_date:
        # Convert UTC timestamp to datetime in local timezone with DST adjustment
        start_date = datetime.utcfromtimestamp(int(start_date) / 1000).replace(tzinfo=ZoneInfo("UTC"))
        start_date = start_date.astimezone(local_timezone).strftime('%Y-%m-%d %H:%M:%S')
        filters.append("t.date >= %s")
        params.append(start_date)

    if end_date:
        # Convert UTC timestamp to datetime in local timezone with DST adjustment
        end_date = datetime.utcfromtimestamp(int(end_date) / 1000).replace(tzinfo=ZoneInfo("UTC"))
        end_date = end_date.astimezone(local_timezone).strftime('%Y-%m-%d %H:%M:%S')
        filters.append("t.date <= %s")
        params.append(end_date)

    # Amount filters
    if min_amount is not None or max_amount is not None:
        amount_filter = """
            t.transactionID IN (
                SELECT ti.transactionID
                FROM transaction_items ti
                LEFT JOIN transactions t2 ON ti.transactionID = t2.transactionID
                GROUP BY ti.transactionID
                HAVING SUM(CASE WHEN t2.direction = 'expense' THEN -ti.amount ELSE ti.amount END)
        """
        if min_amount is not None:
            amount_filter += " >= %s"
            params.append(min_amount)
        if max_amount is not None:
            amount_filter += " AND SUM(CASE WHEN t2.direction = 'expense' THEN -ti.amount ELSE ti.amount END) <= %s"
            params.append(max_amount)
        amount_filter += ")"
        filters.append(amount_filter)

    # Counterparty filters
    if counterparties:
        counterparty_list = counterparties.split(',')
        inclusion_operator = "IN" if counterparty_inclusion_mode else "NOT IN"
        filters.append(f"t.counterparty {inclusion_operator} ({','.join(['%s'] * len(counterparty_list))})")
        params.extend(counterparty_list)

    # Category filters
    if categories:
        category_list = categories.split(',')
        inclusion_operator = "IN" if category_inclusion_mode else "NOT IN"
        filters.append(f"ti.category {inclusion_operator} ({','.join(['%s'] * len(category_list))})")
        params.extend(category_list)

    # Combine filters
    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    # Construct query
    if sort_by == "amount":
        transaction_query = f"""
            SELECT t.transactionID
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transactionID = ti.transactionID
            {where_clause}
            GROUP BY t.transactionID
            ORDER BY SUM(CASE WHEN t.direction = 'expense' THEN -ti.amount ELSE ti.amount END) {sort_order}
            LIMIT %s OFFSET %s
        """
    elif sort_by == "category":
        transaction_query = f"""
            SELECT t.transactionID
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transactionID = ti.transactionID
            LEFT JOIN (
                SELECT transactionID,
                    GROUP_CONCAT(category ORDER BY itemID) AS category
                FROM transaction_items
                GROUP BY transactionID
            ) AS first_category ON t.transactionID = first_category.transactionID
            {where_clause}
            GROUP BY t.transactionID
            ORDER BY first_category.category {sort_order}
            LIMIT %s OFFSET %s
        """
    else:
        transaction_query = f"""
            SELECT t.transactionID
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transactionID = ti.transactionID
            {where_clause}
            GROUP BY t.transactionID
            ORDER BY {sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """

    # Add limit and offset to parameters
    params.extend([limit, offset])

    # Fetch transaction IDs
    transaction_ids = query_mysql(transaction_query, params)
    if not transaction_ids:
        return {"transactions": []}

    # Extract transaction IDs
    transaction_ids_list = [t[0] for t in transaction_ids]

    # Fetch transaction items
    placeholders = ','.join(['%s'] * len(transaction_ids_list))
    items_query = f"""
        SELECT t.transactionID, t.direction, t.date, t.counterparty, t.notes, ti.category, ti.amount
        FROM transactions t
        LEFT JOIN transaction_items ti ON t.transactionID = ti.transactionID
        WHERE t.transactionID IN ({placeholders})
        ORDER BY FIELD(t.transactionID, {','.join(['%s'] * len(transaction_ids_list))})
    """
    items_params = transaction_ids_list + transaction_ids_list
    transactions_items = query_mysql(items_query, items_params)

    # Query for the total amount of transactions that match our filters and compare to the limit
    total_query = f"""
        SELECT COUNT(DISTINCT t.transactionID)
        FROM transactions t
        LEFT JOIN transaction_items ti ON t.transactionID = ti.transactionID
        {where_clause}
    """
    # Make a copy of the params list and remove the limit and offset
    params_for_total = params[:-2]
    # Query and calculate result
    total_count = query_mysql(total_query, params_for_total)
    hasMore = total_count[0][0] > (limit + offset)

    # Organize and process transactions
    transactions_dict = {}
    for transaction in transactions_items:
        transactionID = transaction[0]
        if transactionID not in transactions_dict:
            transactions_dict[transactionID] = {
                "transactionID": transactionID,
                "direction": transaction[1],
                "date": transaction[2],
                "counterparty": transaction[3],
                "notes": transaction[4],
                "categories": [],
                "amount_sum": 0,
            }
        transactions_dict[transactionID]["categories"].append({
            "category": transaction[5],
            "amount": transaction[6]
        })

    # Calculate total amounts
    # Is this needed anymore or is it deprecated?
    for transaction in transactions_dict.values():
        transaction["amount_sum"] = sum(item["amount"] for item in transaction["categories"])

    return {
        "transactions": list(transactions_dict.values()),
        "hasMore": hasMore,
        "offset": offset,
    }


@app.get("/get_options")
def get_options(
    session_key: str = Query(None)
):
    # Validate the session key
    userID = validateSessionKey(session_key, False)

    # Counterparty query 
    counterparty_query = """
        SELECT counterparty, direction
        FROM transactions
        WHERE userID = %s
        GROUP BY counterparty, direction
        ORDER BY COUNT(*) DESC;
    """
    counterpartyValuesObject = query_mysql(counterparty_query, (userID,))
    # Split into expense and income arrays based on the direction
    counterpartyExpense = [row[0] for row in counterpartyValuesObject if row[1] == "expense"]
    counterpartyIncome = [row[0] for row in counterpartyValuesObject if row[1] == "income"]

    # Category query with userID filter
    category_query = """
        SELECT ti.category, t.direction
        FROM transaction_items ti
        JOIN transactions t ON ti.transactionID = t.transactionID
        WHERE t.userID = %s
        GROUP BY ti.category, t.direction
        ORDER BY COUNT(*) DESC;
    """
    categoryValuesObject = query_mysql(category_query, (userID,))
    # Split into expense and income arrays based on the direction
    categoryExpense = [row[0] for row in categoryValuesObject if row[1] == "expense"]
    categoryIncome = [row[0] for row in categoryValuesObject if row[1] == "income"]

    return {"counterparty": {"expense": counterpartyExpense, "income": counterpartyIncome},
            "category": {"expense": categoryExpense, "income": categoryIncome}}


@app.get("/get_filters")
def get_filters(
    session_key: str = Query(None)
):

    # Validate the session key
    userID = validateSessionKey(session_key, False)

    try:
        # Counterparty query with userID filter
        counterparty_query = """
            SELECT counterparty, direction
            FROM transactions
            WHERE userID = %s
            GROUP BY counterparty, direction
            ORDER BY COUNT(*) DESC;
        """
        counterpartyValuesObject = query_mysql(counterparty_query, (userID,))
        # Split into expense and income arrays based on the direction
        counterpartyExpense = [row[0] for row in counterpartyValuesObject if row[1] == "expense"]
        counterpartyIncome = [row[0] for row in counterpartyValuesObject if row[1] == "income"]

        # Category query with userID filter
        category_query = """
            SELECT ti.category, t.direction
            FROM transaction_items ti
            JOIN transactions t ON ti.transactionID = t.transactionID
            WHERE t.userID = %s
            GROUP BY ti.category, t.direction
            ORDER BY COUNT(*) DESC;
        """
        categoryValuesObject = query_mysql(category_query, (userID,))
        # Split into expense and income arrays based on the direction
        categoryExpense = [row[0] for row in categoryValuesObject if row[1] == "expense"]
        categoryIncome = [row[0] for row in categoryValuesObject if row[1] == "income"]

        # Query for min and max dates as UNIX timestamps
        date_query = """
            SELECT 
                UNIX_TIMESTAMP(MIN(date)) AS minDate, 
                UNIX_TIMESTAMP(MAX(date)) AS maxDate
            FROM transactions
            WHERE userID = %s;
        """
        dateValues = query_mysql(date_query, (userID,))
        minDate = dateValues[0][0]
        maxDate = dateValues[0][1]

        # Query for max and min amounts, adjusting for direction, with userID filter
        amount_query = """
            SELECT MAX(adjusted_amount) AS maxAmount, MIN(adjusted_amount) AS minAmount
            FROM (
                SELECT t.transactionID, 
                    SUM(CASE 
                        WHEN t.direction = 'expense' THEN -ti.amount
                        WHEN t.direction = 'income' THEN ti.amount
                    END) AS adjusted_amount
                FROM transaction_items ti
                JOIN transactions t ON ti.transactionID = t.transactionID
                WHERE t.userID = %s
                GROUP BY t.transactionID
            ) AS transaction_totals;
        """
        amountValues = query_mysql(amount_query, (userID,))
        minAmount = amountValues[0][1]
        maxAmount = amountValues[0][0]

        return {
            "counterparty": {
                "expense": counterpartyExpense,
                "income": counterpartyIncome
            },
            "category": {
                "expense": categoryExpense,
                "income": categoryIncome
            },
            "amount": {
                "min": float(minAmount),
                "max": float(maxAmount)
            },
            "date": {
                "min": minDate * 1000,
                "max": maxDate * 1000
            }
        }

    except Exception as e:
        return {"error": str(e)}


# - - - - - - - - - - - - - - - - -  - - - - - - - #
# - - - - - - - CHARTS AND ANALYTICS - - - - - - - #
# - - - - - - - - - - - - - - - - -  - - - - - - - #
@app.post("/get_chart/balance_over_time")
def get_chart_balance_over_time(data: dict):
    try:

        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, False)

        # Get initial balance from the request data
        initial_balance = data.get("initial_balance", 0)  # Default to 0 if not provided
        # If the user hasn't set a value for initial_balance, it will be None.
        # This caused some headaches, so do not remove this check
        if initial_balance is None:
            initial_balance = 0

        # Query for the balance over time, but do not return the daily_balance
        balance_query = """
            SELECT 
                date,
                @running_balance := CAST(@running_balance + daily_balance AS DECIMAL(10, 2)) AS running_balance
            FROM (
                SELECT 
                    t.date,
                    SUM(CASE WHEN t.direction = 'income' THEN ti.amount ELSE -ti.amount END) AS daily_balance
                FROM 
                    transactions t
                JOIN 
                    transaction_items ti ON t.transactionID = ti.transactionID
                WHERE 
                    t.userID = %s
                GROUP BY 
                    t.date
                ORDER BY 
                    t.date
            ) daily_balances
            JOIN 
                (SELECT @running_balance := CAST(%s AS DECIMAL(10, 2))) r;  -- Using initial_balance here
        """
        balance_result = query_mysql(balance_query, (user_id, initial_balance))  # Pass initial_balance in the query

        # If there are results, fill in the gaps
        if balance_result:
            filled_balance_result = []
            previous_date = None
            previous_balance = initial_balance

            for row in balance_result:
                current_date = row[0]
                current_balance = row[1]

                # Fill in missing dates (if there was a gap)
                if previous_date and (current_date - previous_date).days > 1:
                    # Insert missing days
                    missing_days = (current_date - previous_date).days - 1
                    for i in range(missing_days):
                        new_date = previous_date + timedelta(days=i + 1)
                        filled_balance_result.append({
                            "date": new_date,
                            "runningBalance": previous_balance
                        })

                # Add the current day's data
                filled_balance_result.append({
                    "date": current_date,
                    "runningBalance": current_balance
                })
                previous_date = current_date
                previous_balance = current_balance

            return {"balanceOverTime": filled_balance_result}

        return {"balanceOverTime": []}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/get_chart/sum_by_month")
def get_chart_sum_by_month(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, False)

        # Query for the monthly sums of income, expense, and their total
        monthly_sum_query = """
            SELECT 
                DATE_FORMAT(t.date, '%Y-%m') AS month,
                SUM(CASE WHEN t.direction = 'income' THEN ti.amount ELSE 0 END) AS total_income,
                SUM(CASE WHEN t.direction = 'expense' THEN ti.amount * -1 ELSE 0 END) AS total_expense,
                SUM(CASE WHEN t.direction = 'income' THEN ti.amount 
                         WHEN t.direction = 'expense' THEN ti.amount * -1 
                         ELSE 0 END) AS net_total
            FROM 
                transactions t
            JOIN 
                transaction_items ti ON t.transactionID = ti.transactionID
            WHERE 
                t.userID = %s
            GROUP BY 
                month
            ORDER BY 
                month;
        """
        monthly_sum_result = query_mysql(monthly_sum_query, (user_id,))

        # Prepare the response
        if monthly_sum_result:
            formatted_result = [
                {
                    "month": row[0],
                    "total_income": float(row[1]),
                    "total_expense": float(row[2]),
                    "net_total": float(row[3]),
                }
                for row in monthly_sum_result
            ]
            return {"monthlySums": formatted_result}

        return {"monthlySums": []}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/get_chart/categories_monthly")
def get_chart_categories_monthly(data: dict):
    try:
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, False)
        direction = data.get("direction", "expense")
        
        # Query for min and max dates across both directions
        date_range_query = """
            SELECT MIN(DATE_FORMAT(date, '%Y-%m')), MAX(DATE_FORMAT(date, '%Y-%m'))
            FROM transactions WHERE userID = %s;
        """
        date_range_result = query_mysql(date_range_query, (user_id,))
        first_month, last_month = date_range_result[0] if date_range_result else (None, None)
        
        if not first_month or not last_month:
            return {"monthlyCategoryExpenses": []}
        
        query = """
            SELECT 
                DATE_FORMAT(t.date, '%Y-%m') AS month,
                ti.category,
                SUM(ti.amount) AS total_expense
            FROM transactions t
            JOIN transaction_items ti ON t.transactionID = ti.transactionID
            WHERE t.userID = %s AND t.direction = %s
            GROUP BY month, ti.category
            ORDER BY month, ti.category;
        """
        results = query_mysql(query, (user_id, direction))
        
        if not results:
            return {"monthlyCategoryExpenses": []}
        
        formatted_result = {}
        categories = set()
        months = set()

        for row in results:
            month, category, total_expense = row[0], row[1], float(row[2])
            categories.add(category)
            months.add(month)
            if month not in formatted_result:
                formatted_result[month] = {}
            formatted_result[month][category] = total_expense
        
        # Determine full month range
        all_months = [m.strftime('%Y-%m') for m in pd.date_range(first_month, last_month, freq='MS')]
        
        final_result = []
        for month in all_months:
            month_data = {"month": month, "categories": []}
            for category in categories:
                month_data["categories"].append({
                    "category": category,
                    "total_expense": formatted_result.get(month, {}).get(category, 0)
                })
            final_result.append(month_data)
        
        # Sort categories by total sum across all months
        category_totals = {category: 0 for category in categories}
        for month_data in final_result:
            for category_data in month_data["categories"]:
                category_totals[category_data["category"]] += category_data["total_expense"]
        for month_data in final_result:
            month_data["categories"].sort(key=lambda x: category_totals[x["category"]], reverse=True)
        
        return {"monthlyCategoryExpenses": final_result}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/analytics/get_general_stats")
def analytics_get_general_stats(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, False)

        # Query for general stats
        general_stats_query = """
            SELECT
                COUNT(*) AS transaction_count,
                DATEDIFF(MAX(date), MIN(date)) + 1 AS days_logged,
                COUNT(*) / NULLIF(DATEDIFF(MAX(date), MIN(date)) + 1, 0) AS avg_logs_per_day,
                SUM(CASE WHEN direction = 'expense' THEN ti.amount ELSE 0 END) AS total_expenses,
                SUM(CASE WHEN direction = 'income' THEN ti.amount ELSE 0 END) AS total_incomes
            FROM 
                transactions t
            JOIN 
                transaction_items ti ON t.transactionID = ti.transactionID
            WHERE 
                t.userID = %s
        """
        general_stats_result = query_mysql(general_stats_query, (user_id,))

        # Prepare the response
        if general_stats_result:
            row = general_stats_result[0]
            result = {
                "transactionsLogged": row[0],
                "daysLogged": row[1],
                "avgLogsPerDay": float(row[2]) if row[2] is not None else 0,
                "totalExpenses": float(row[3]),
                "totalIncomes": float(row[4]),
            }
            return {"generalStats": result}

        return {"generalStats": {}}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/analytics/get_stats_for_timespan")
def analytics_get_last_timespan_stats(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key, False)

        # Get timespan
        timespan = data.get("timespan")
        if timespan not in ["month", "year"]:
            raise HTTPException(status_code=403, detail="Invalid or missing timespan. Allowed values: 'month', 'year'.")

        # Define the date range based on the timespan
        if timespan == "month":
            today = date.today()
            prev_month = today.month - 1 or 12
            year = today.year if today.month > 1 else today.year - 1

            # Get the number of days in the last month
            days_in_period = calendar.monthrange(year, prev_month)[1]

            # Calculate weeks (approximating to full weeks)
            weeks_in_period = days_in_period / 7

            # Months are fixed to 1 for this timespan
            months_in_period = 1

            date_condition = "t.date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
        elif timespan == "year":
            # Check for leap year
            days_in_period = 366 if calendar.isleap(date.today().year) else 365

            # Fixed
            weeks_in_period = 52

            # Fixed
            months_in_period = 12
            date_condition = "t.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)"

        # print("Days in period: ")
        # print(days_in_period)
        # print("Weeks in period: ")
        # print(weeks_in_period)
        # print("Months in period: ")
        # print(months_in_period)

        # Query for total expenses within the timespan
        stats_query = f"""
            SELECT
                SUM(CASE WHEN t.direction = 'expense' THEN ti.amount ELSE 0 END) AS total_expenses
            FROM 
                transactions t
            JOIN 
                transaction_items ti ON t.transactionID = ti.transactionID
            WHERE 
                t.userID = %s AND {date_condition}
        """
        stats_result = query_mysql(stats_query, (user_id,))

        # Query for the expenses and incomes ratio
        ratio_query = f"""
            SELECT
                COALESCE(SUM(CASE WHEN t.direction = 'income' THEN ti.amount ELSE 0 END), 0) AS total_incomes,
                COALESCE(SUM(CASE WHEN t.direction = 'expense' THEN ti.amount ELSE 0 END), 0) AS total_expenses
            FROM 
                transactions t
            JOIN 
                transaction_items ti ON t.transactionID = ti.transactionID
            WHERE 
                t.userID = %s AND {date_condition};
        """
        ratio_result = query_mysql(ratio_query, (user_id,))

        # Query for the originally just 5 most common expense categories
        # category_query = f"""
        #     SELECT 
        #         ti.category, 
        #         COUNT(*) AS count 
        #     FROM 
        #         transactions t
        #     JOIN 
        #         transaction_items ti ON t.transactionID = ti.transactionID
        #     WHERE 
        #         t.userID = %s AND t.direction = 'expense' AND {date_condition}
        #     GROUP BY 
        #         ti.category
        #     ORDER BY 
        #         count DESC;
        # """
        # category_result = query_mysql(category_query, (user_id,))

        # Query for the originally just 5 most expensive expense categories by total sum
        category_avg_by_month_query = f"""
            SELECT 
                ti.category,
                SUM(ti.amount) AS total_amount
            FROM 
                transactions t
            JOIN 
                transaction_items ti ON t.transactionID = ti.transactionID
            WHERE 
                t.userID = %s AND t.direction = 'expense' AND {date_condition}
            GROUP BY 
                ti.category
            ORDER BY 
                total_amount DESC;
        """

        expensive_result = query_mysql(category_avg_by_month_query, (user_id,))

        # Prepare the response
        if stats_result:

            # Spendings avg timespan
            total_expenses = stats_result[0][0] or 0
            spendings_avg_day = float(total_expenses) / days_in_period if days_in_period else 0
            spendings_avg_week = float(total_expenses) / weeks_in_period if weeks_in_period else 0
            spendings_avg_month = float(total_expenses) / months_in_period if months_in_period else 0

            # Handle the ratio calculation
            total_incomes = float(ratio_result[0][0]) if ratio_result and ratio_result[0][0] is not None else 0
            total_expenses = float(ratio_result[0][1]) if ratio_result and ratio_result[0][1] is not None else 0
            income_expense_ratio = (total_incomes / total_expenses) if total_expenses else None
            net_total = total_incomes - total_expenses

            # Prepare the most common categories
            # common_categories = [
            #     {"category": row[0], "count": row[1]} for row in category_result
            # ] if category_result else []

            # Prepare avg by category
            if timespan == "month":
                spendings_avg_month_by_category = [
                    {"category": row[0], "totalAmount": float(row[1])}
                    for row in expensive_result
                ] if expensive_result else []
            elif timespan == "year":
                spendings_avg_month_by_category = [
                    {"category": row[0], "totalAmount": float(row[1] / 12)}
                    for row in expensive_result
                ] if expensive_result else []

            result = {
                "spendingsAverageDay": spendings_avg_day,
                "spendingsAverageWeek": spendings_avg_week,
                "spendingsAverageMonth": spendings_avg_month,
                "incomeExpenseRatio": income_expense_ratio,
                "netTotal": net_total,
                # "topMostCommonCategories": common_categories,
                "topMostExpensiveCategories": spendings_avg_month_by_category,
            }
            return {"stats": result}

        # Default response if no data
        return {
            "stats": {
                "spendingsAverageDay": 0,
                "spendingsAverageWeek": 0,
                "spendingsAverageMonth": 0,
                "incomeExpenseRatio": None,  # Explicitly indicate missing ratio
                "netTotal": 0,
                # "topMostCommonCategories": [],
                "topMostExpensiveCategories": [],
            }
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# - - - - - - - - - - - - - - - -  - - - - - - - #
# - - - - - - - BACKUPS AND DRIVES - - - - - - - #
# - - - - - - - - - - - - - - - -  - - - - - - - #
@app.post("/get_server_drives_info")
def get_server_drives_info():
    try:
        # Define each folder with a name and path
        driveFolders = [
            {"name": "Boot drive", "path": "/driveBoot"},
            {"name": "Mass storage drive", "path": "/driveData"}
        ]
        drive_info = []

        for folder in driveFolders:
            # Get disk usage for the folder's path
            usage = psutil.disk_usage(folder["path"])

            drive_info.append({
                "name": folder["name"],
                "total": usage.total,
                "used": usage.used,
                "free": usage.free,
            })

        return drive_info
    except Exception as e:
        return {"error": str(e)}


@app.post("/log_backup")
def log_backup(data: dict):

    # Check and get backup_name
    backup_name = data.get("backup_name")
    if not backup_name:
        raise HTTPException(status_code=403, detail="Missing backup_name param.")
    
    # Update the last_success column for the specified backup name
    query = """
        UPDATE backups
        SET last_success = NOW()
        WHERE backup_name = %s
    """
    query_mysql(query, (backup_name,))
    
    return {"message": "Logged successfully"}


@app.get("/get_backups")
def get_backups():
    query = "SELECT backup_id, backup_name, backup_direction, backup_category, peer_device, source_path, destination_path, last_success FROM backups"
    
    backups = query_mysql(query)
    
    if backups:
        formatted_backups = defaultdict(list)  # Group backups by category
        for backup in backups:
            # Parse the last_success timestamp
            last_success = backup[7]
            if last_success:
                time_since = datetime.now() - last_success
                last_success_time_since = format_time_difference(time_since)
                last_success_in_hours = round(time_since.total_seconds() / 3600, 2)

                # A custom thershold for the status when using the air gapped drive
                thresholds = {
                    "Old laptop hdd": [24 * 7 * (52 / 4), 24 * 7 * (52 / 2)],
                    "default": [24, 72]
                }
                device_thresholds = thresholds.get(backup[1], thresholds["default"])

                if last_success_in_hours < device_thresholds[0]:
                    status = "good"
                elif last_success_in_hours < device_thresholds[1]:
                    status = "warning"
                else:
                    status = "bad"
            else:
                last_success_time_since = "Never"
                status = "bad"
            
            # Format the backup direction (schedule)
            direction = backup[2]
            if backup[1] == 'Old laptop hdd':
                schedule = "-"
            elif direction == 'up':
                schedule = "Päivittäin, 4.00"  # Daily at 4.00
            elif direction == 'down':
                schedule = "Päivittäin, 5.00"  # Daily at 5.00
            else:
                schedule = "-"
            
            # Calculate time until the next backup
            if (backup[1] != 'Old laptop hdd'):
                now = datetime.now()
                if direction == 'up':
                    # Next backup scheduled for 4:00 AM today or tomorrow
                    next_backup_time = datetime(now.year, now.month, now.day, 4, 0)  # 4:00 AM today
                    if now > next_backup_time:
                        # If it's already past 4:00 AM, schedule for 4:00 AM tomorrow
                        next_backup_time += timedelta(days=1)
                elif direction == 'down':
                    # Next backup scheduled for 5:00 AM today or tomorrow
                    next_backup_time = datetime(now.year, now.month, now.day, 5, 0)  # 5:00 AM today
                    if now > next_backup_time:
                        # If it's already past 5:00 AM, schedule for 5:00 AM tomorrow
                        next_backup_time += timedelta(days=1)
                else:
                    return "Invalid direction"  # In case of an unexpected direction

                # Calculate the time difference between now and the next scheduled time
                time_until_next = next_backup_time - now
                time_until_next_str = format_time_difference(time_until_next)
            else:
                time_until_next_str = "-"
            
            # Format the backup data
            formatted_backup = {
                "backup_name": backup[1],
                "backup_direction": direction,
                "peer_device": backup[4],
                "schedule": schedule,
                "status": status,
                # Paths
                "source_path": backup[5],
                "destination_path": backup[6],
                # Times since and until
                "last_success_time_since": last_success_time_since,
                "time_until_next": time_until_next_str,
            }
            
            # Group backups by their category
            formatted_backups[backup[3]].append(formatted_backup)
        
        return {"backups": formatted_backups}
    else:
        raise HTTPException(status_code=404, detail="No backups found.")

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


# - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - TV AND MOVIE WATCH LIST - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - #

# Used to manually update the genres if they change etc. In the past was ran always on start, but since it ran on all 4 workers the feature was removed.
@app.get("/watch_list/udpate_genres")
def fetch_genres():
    # Fetch movie genres
    movie_genres = query_tmdb("/genre/movie/list", {})
    if movie_genres:
        for genre in movie_genres.get("genres", []):
            genre_id = genre.get("id")
            genre_name = genre.get("name")
            # Check if genre exists in the database
            existing_genre = query_mysql("SELECT * FROM genres WHERE tmdb_genre_id = %s", (genre_id,))
            if not existing_genre:
                # Insert new genre
                query_mysql("INSERT INTO genres (tmdb_genre_id, genre_name) VALUES (%s, %s)", (genre_id, genre_name))
            else:
                # Update genre if name changes
                query_mysql("UPDATE genres SET genre_name = %s WHERE tmdb_genre_id = %s", (genre_name, genre_id))
        print("Movie genres stored!")

    # Fetch TV genres
    tv_genres = query_tmdb("/genre/tv/list", {})
    if tv_genres:
        for genre in tv_genres.get("genres", []):
            genre_id = genre.get("id")
            genre_name = genre.get("name")
            # Check if genre exists in the database
            existing_genre = query_mysql("SELECT * FROM genres WHERE tmdb_genre_id = %s", (genre_id,))
            if not existing_genre:
                # Insert new genre
                query_mysql("INSERT INTO genres (tmdb_genre_id, genre_name) VALUES (%s, %s)", (genre_id, genre_name))
            else:
                # Update genre if name changes
                query_mysql("UPDATE genres SET genre_name = %s WHERE tmdb_genre_id = %s", (genre_name, genre_id))
        print("TV genres stored!")

    return {"Result": "Genres updated!",}


# ADD A VALUE BASED ON IF THE USER HAS THE TITLE ADDED
# ADD A VALUE BASED ON IF THE USER HAS THE TITLE ADDED
# ADD A VALUE BASED ON IF THE USER HAS THE TITLE ADDED
# ADD A VALUE BASED ON IF THE USER HAS THE TITLE ADDED
# |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
# |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
# v  v  v  v  v  v  v  v  v  v  v  v  v  v  v  v  v  v

@app.get("/watch_list/search")
def watch_list_search(
    title_category: str = Query(..., regex="^(Movie|TV)$"),
    title_name: str = Query(None)  # Optional
):
    global tempCachedSearchResults  # Declare it as a global variable

    # If title_name is provided, query TMDb for search results
    if title_name:
        search_results = query_tmdb(f"/search/{title_category.lower()}", {"query": title_name, "include_adult": True})
        # tempCachedSearchResults = search_results  # Cache the results
    # If title_name is not provided, return cached results
    elif 'tempCachedSearchResults' in globals():
        search_results = tempCachedSearchResults
    else:
        # In case no cached results exist, raise an error or provide a fallback
        raise HTTPException(status_code=400, detail="Title name is required or cached data is unavailable.")

    # Fetch genres from MySQL based on title_category
    genre_query = "SELECT tmdb_genre_id, genre_name FROM genres"

    genre_data = query_mysql(genre_query, ())  # Fetch genres from MySQL
    if not genre_data:
        raise HTTPException(status_code=500, detail="Genres not found in the database.")
    
    # Assuming query_mysql returns a list of tuples, convert it into a dictionary
    genre_dict = {genre[0]: genre[1] for genre in genre_data}  # genre[0] is tmdb_genre_id, genre[1] is genre_name

    # Replace genre IDs with genre names in search results
    for result in search_results.get('results', []):
        result['genres'] = [genre_dict.get(genre_id, "Unknown") for genre_id in result.get('genre_ids', [])]
    
    return search_results
