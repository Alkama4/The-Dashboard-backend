# Standard libraries
import asyncio
import calendar
import os
import random
import string
import time
from collections import defaultdict, OrderedDict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Literal

# Third party libraries (most likely need prestart install)
import mysql.connector
import pandas as pd
import psutil
import httpx

# Fast api imports
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# Create fastAPI instance and set CORS middleware
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # List of allowed origins
    allow_methods=["*"],      # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],      # Allow all headers
)

# Keep a couple copies of the search results cached for the duration of the server runtime
# Might want to replace this with a mysql table wit BLOBs in the future because of workers.
tmdbQueryCacheMaxSize = 5
tmdbQueryCache = OrderedDict()

# - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - BASIC TOOLS - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - #

# Function to connect to MySQL and perform a query
def query_mysql(query: str, params: tuple = (), fetch_last_row_id=False):
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

        cursor = conn.cursor()

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

        # Handle insert or other queries without return value
        conn.commit()
        cursor.close()
        conn.close()

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


# Download an image from an url, semaphore to limit the amount of async tasks.
async def download_image(image_url: str, image_save_path: str):
    try:
        # Skip download if file already exists
        if os.path.exists(image_save_path):
            print(f"Image already exists: {image_save_path}, skipping download.")
            return

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


# Add search queries to cache since it takes so long to fetch them
def add_to_cache(key, value):
    global tmdbQueryCache
    
    # If the cache is full, pop the oldest item
    if len(tmdbQueryCache) >= tmdbQueryCacheMaxSize:
        tmdbQueryCache.popitem(last=False)  # Remove the oldest item
    
    # Add the new item to the cache
    tmdbQueryCache[key] = value


# Is ran everytime an endpoint is called
# Used to log request for analysis
@app.middleware("http")
async def log_request_data(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    endpoint = request.url.path
    status_code = response.status_code
    client_ip = request.client.host
    method = request.method

    insert_query = """
    INSERT INTO server_fastapi_request_logs (endpoint, status_code, backend_time_ms, client_ip, method)
    VALUES (%s, %s, %s, %s, %s)
    """
    query_mysql(insert_query, (endpoint, status_code, process_time * 1000, client_ip, method))

    return response


# Landing page that shows what is available (SUPER OUT OF DATE)
@app.get("/")
def root(request: Request):
    return {
        "Head": "Hello world!",
        "Text": "Welcome to the FastAPI backend for the Vue.js frontend. The available endpoints are listed below.",
        "Note": "This isn't updated regularly and is out of date",
        "Request came from": request.client.host,
        # "Request headers": request.headers,
        # "forwarded_for": request.headers.get("x-forwarded-for"),
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


@app.get("/get_login_status")
def get_login_status(
    session_key: str = Query(...)
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
    result = query_mysql(query, (session_key,))
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
@app.post("/transactions/new_transaction")
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
        transaction_id = query_mysql(transaction_query, (direction, date, counterparty, notes, user_id), True)

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


@app.post("/transactions/edit_transaction")
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
        
        # print(date)

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


# This should be DELETE but thats a later me problem
@app.post("/transactions/delete_transaction")
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


@app.get("/transactions/get_transactions")
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


@app.get("/transactions/get_options")
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


@app.get("/transactions/get_filters")
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
@app.get("/get_chart/balance_over_time")
def get_chart_balance_over_time(
    session_key: str = Query(None),
    initial_balance: int = Query(0)
):
    try:
        # Validate the session key
        user_id = validateSessionKey(session_key, False)

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


@app.get("/get_chart/sum_by_month")
def get_chart_sum_by_month(
    session_key: str = Query(None),
):
    try:
        # Validate the session key
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


@app.get("/get_chart/categories_monthly")
def get_chart_categories_monthly(
    session_key: str = Query(None),
    direction: Literal['expense', 'income'] = Query(default='expense')
):
    try:
        user_id = validateSessionKey(session_key, False)
        
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


@app.get("/analytics/get_general_stats")
def analytics_get_general_stats(
    session_key: str = Query(None),
):
    try:
        # Validate the session key
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


@app.get("/analytics/get_stats_for_timespan")
def analytics_get_last_timespan_stats(
    session_key: str = Query(None),
    timespan: str = Query(None),
):
    try:
        # Validate the session key
        user_id = validateSessionKey(session_key, False)

        # Get timespan
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


# - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - BACKUPS AND SERVER INFO - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - #
@app.get("/get_server_drives_info")
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


@app.post("/store_server_resource_logs")
def store_server_resource_logs(data: dict):
    try:
        store_data_query = """
            INSERT INTO server_resource_logs (
                cpu_temperature,
                cpu_usage,
                ram_usage,
                disk_usage,
                system_load,
                network_sent_bytes,
                network_recv_bytes
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        store_data_params = (
            data["cpu_temperature"],
            data["cpu_usage"],
            data["ram_usage"],
            data["disk_usage"],
            data["system_load"],
            data["network_sent_bytes"],
            data["network_recv_bytes"],
        )
        query_mysql(store_data_query, store_data_params)
        return {"message": "Success"}
    
    except Exception as e:
        print(e)
        return {"error": str(e)}
    

@app.get("/get_server_resource_logs")
def get_server_resource_logs(timeframe: str = Query(None)):
    try:
        # Build the WHERE clause based on the timeframe
        if timeframe == "24h":
            where_clause = "timestamp >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)"
        elif timeframe == "7d":
            where_clause = "timestamp >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 7 DAY)"
        elif timeframe == "30d":
            where_clause = "timestamp >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY)"
        else:
            raise HTTPException(status_code=400, detail="Invalid timeframe")

        # SQL query with dynamic WHERE clause
        server_data_query = f"""
            SELECT 
                cpu_temperature,
                cpu_usage,
                ram_usage,
                disk_usage,
                system_load,
                network_sent_bytes,
                network_recv_bytes,
                timestamp
            FROM server_resource_logs
            WHERE {where_clause}
        """
        
        # Execute the query and fetch the result
        server_data_result = query_mysql(server_data_query, ())

        # Format the results so that we have objects instead of arrays
        formatted_data = [
            {
                "cpu_temperature": result[0],
                "cpu_usage": result[1],
                "ram_usage": result[2],
                "disk_usage": result[3],
                "system_load": result[4],
                "network_sent_bytes": result[5],
                "network_recv_bytes": result[6],
                "timestamp": result[7]
            } 
            for result in server_data_result
        ]
        
        return {"data": formatted_data}
    
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


# - - - - - - - - - - - - - - - - - - - - - - - - - - #
# - - - - - - - TV AND MOVIE WATCH LIST - - - - - - - #
# - - - - - - - - - - - - - - - - - - - - - - - - - - #

# Used to manually update the genres if they change etc. In the past was ran always on start, but since it ran on all 4 workers the feature was removed. Basically only used if I were to wipe the whole db
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


# Acts as a middle man between TMDB search and vue. 
# Adds proper genres and the fact wether the user has added the title or not.
@app.get("/watch_list/search")
def watch_list_search(
    session_key: str = Query(...),
    title_category: str = Query(..., regex="^(Movie|TV)$"),
    title_name: str = Query(None),
):
    global tmdbQueryCache

    # Validate the session key and retrieve the user ID
    user_id = validateSessionKey(session_key, False)

    # Fetch search results from cache or TMDB API
    if title_name:
        title_lower = title_name.lower()
        search_results = tmdbQueryCache.get(title_lower)
        if search_results is None:
            search_results = query_tmdb(
                f"/search/{title_category.lower()}",
                {"query": title_name, "include_adult": False}
            )
            add_to_cache(title_lower, search_results)  # Store results in cache
    else:
        raise HTTPException(status_code=400, detail="Title name is required.")

    # Retrieve genre mappings from the database
    genre_query = "SELECT tmdb_genre_id, genre_name FROM genres"
    genre_data = query_mysql(genre_query, ())
    if not genre_data:
        raise HTTPException(status_code=500, detail="Genres not found in the database.")
    genre_dict = {genre[0]: genre[1] for genre in genre_data}

    # Get the TMDB IDs from search results
    tmdb_ids = [result.get('id') for result in search_results.get('results', [])]

    # Fetch watchlist details (title_id) for the user
    if tmdb_ids:
        placeholders = ', '.join(['%s'] * len(tmdb_ids))

        # Query to get title_id based on tmdb_id from the titles table
        title_id_query = f"""
            SELECT tmdb_id, title_id
            FROM titles
            WHERE tmdb_id IN ({placeholders})
        """
        title_id_data = query_mysql(title_id_query, (*tmdb_ids,))
        title_id_dict = {row[0]: row[1] for row in title_id_data}

        # Query to get user's watchlist details
        watchlist_query = f"""
            SELECT t.tmdb_id, t.title_id
            FROM user_title_details utd
            JOIN titles t ON utd.title_id = t.title_id
            WHERE utd.userID = %s AND t.tmdb_id IN ({placeholders})
        """
        watchlist_data = query_mysql(watchlist_query, (user_id, *tmdb_ids))
        watchlist_dict = {row[0]: row[1] for row in watchlist_data}
    else:
        title_id_dict = {}
        watchlist_dict = {}

    # Process search results: add genre names, watchlist status, and title_id
    for result in search_results.get('results', []):
        result['genres'] = [genre_dict.get(genre_id, "Unknown") for genre_id in result.get('genre_ids', [])]
        tmdb_id = result.get('id')
        result['title_id'] = title_id_dict.get(tmdb_id)
        result['in_watch_list'] = tmdb_id in watchlist_dict

    return search_results


# Used to get the images for a title and only the title. Seasons and episodes have a seperate one
async def store_title_images(movie_images, title_id: str):
    try:
        base_path = f'/fastapi-images/{title_id}'
        Path(base_path).mkdir(parents=True, exist_ok=True)

        tasks = []

        # Get the first logo
        if 'logos' in movie_images:
            logo = movie_images['logos'][:1]  # Get only the first logo
            for idx, image in enumerate(logo):
                image_url = f"https://image.tmdb.org/t/p/original{image['file_path']}"
                file_extension = image['file_path'].split('.')[-1]
                image_filename = f"logo.{file_extension}"
                image_save_path = os.path.join(base_path, image_filename)
                tasks.append(download_image(image_url, image_save_path))

        # Get the first poster
        if 'posters' in movie_images:
            # Filter posters to only include those with "iso_639_1" as "en"
            english_posters = [image for image in movie_images['posters'] if image.get('iso_639_1') == 'en']
            
            if english_posters:  # Check if there are any English posters
                first_english_poster = english_posters[0]  # Get the first English poster
                image_url = f"https://image.tmdb.org/t/p/original{first_english_poster['file_path']}"
                file_extension = first_english_poster['file_path'].split('.')[-1]
                image_filename = f"poster.{file_extension}"
                image_save_path = os.path.join(base_path, image_filename)
                tasks.append(download_image(image_url, image_save_path))

        # Get the first 5 backdrops
        if 'backdrops' in movie_images:
            backdrops = movie_images['backdrops'][:5]  # Get the first 5 backdrops
            for idx, image in enumerate(backdrops):
                image_url = f"https://image.tmdb.org/t/p/original{image['file_path']}"
                file_extension = image['file_path'].split('.')[-1]
                image_filename = f"backdrop{idx + 1}.{file_extension}"
                image_save_path = os.path.join(base_path, image_filename)
                tasks.append(download_image(image_url, image_save_path))

        # Run all the download tasks concurrently
        await asyncio.gather(*tasks)

        return {"success": True}

    except Exception as e:
        print(f"store_title_images error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def store_season_images(tv_seasons, title_id: str):
    try:
        tasks = []

        for season in tv_seasons:
            season_number = season.get("season_number")
            poster_path = season.get("poster_path")

            if not poster_path or season_number == 0:  # Skip if no poster exists or if specials
                continue

            # Define base path for season
            season_path = f'/fastapi-images/{title_id}/season{season_number}'
            Path(season_path).mkdir(parents=True, exist_ok=True)

            # Construct image URL & save path
            image_url = f"https://image.tmdb.org/t/p/original{poster_path}"
            file_extension = poster_path.split('.')[-1]
            image_filename = f"poster.{file_extension}"
            image_save_path = os.path.join(season_path, image_filename)

            # Add download task
            tasks.append(download_image(image_url, image_save_path))

        # Run all the download tasks concurrently
        await asyncio.gather(*tasks)

        return {"success": True}

    except Exception as e:
        print(f"store_season_images error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def store_episode_images(tv_episodes, title_id: str):
    try:
        tasks = []

        for episode in tv_episodes:
            season_number = episode.get("season_number")
            episode_number = episode.get("episode_number")
            still_path = episode.get("still_path")

            if not still_path:  # Skip if no still image exists
                continue

            # Define base path for the episode image
            episode_path = f'/fastapi-images/{title_id}/season{season_number}'
            Path(episode_path).mkdir(parents=True, exist_ok=True)

            # Construct image URL & save path
            image_url = f"https://image.tmdb.org/t/p/original{still_path}"
            file_extension = still_path.split('.')[-1]
            image_filename = f"episode{episode_number}.{file_extension}"
            image_save_path = os.path.join(episode_path, image_filename)

            # Add download task
            tasks.append(download_image(image_url, image_save_path))

        # Run all the download tasks concurrently
        await asyncio.gather(*tasks)

        return {"success": True}

    except Exception as e:
        print(f"store_episode_images error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Used for the tvs and movies to add the genres to avoid duplication
def add_or_update_genres_for_title(title_id, tmdb_genres):
    if not tmdb_genres:
        return  # No genres to process

    # Fetch genre IDs from the database
    genre_query = "SELECT tmdb_genre_id, genre_id FROM genres WHERE tmdb_genre_id IN (%s)" % ','.join(
        str(genre['id']) for genre in tmdb_genres
    )
    result = query_mysql(genre_query)

    # Map TMDB genre ID to local genre ID
    genre_ids = {row[0]: row[1] for row in result}

    # Remove existing genre associations for this title_id
    delete_genre_query = "DELETE FROM title_genres WHERE title_id = %s"
    query_mysql(delete_genre_query, (title_id,))

    # Insert new genre associations into title_genres
    genre_values = ", ".join(f"({title_id}, {genre_ids[genre['id']]})" for genre in tmdb_genres if genre['id'] in genre_ids)
    
    if genre_values:
        insert_genre_query = f"INSERT INTO title_genres (title_id, genre_id) VALUES {genre_values}"
        query_mysql(insert_genre_query)

# Functions for the actual adding/updating of a movie or a tv-show
async def add_or_update_movie_title(title_tmdb_id):
    try:
        # Get the data from TMDB
        movie_title_info = query_tmdb(f"/movie/{title_tmdb_id}", {
            "append_to_response": "images,releases,videos",
            "include_image_language": "en,null",
        })

        # Insert the movie into titles
        query = """
            INSERT INTO titles (
                tmdb_id, 
                imdb_id, 
                type, 
                title_name, 
                title_name_original, 
                tagline, 
                vote_average, 
                vote_count, 
                overview, 
                poster_url, 
                backdrop_url, 
                movie_runtime, 
                release_date,
                original_language,
                age_rating,
                trailer_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                imdb_id = VALUES(imdb_id),
                title_name = VALUES(title_name),
                title_name_original = VALUES(title_name_original),
                tagline = VALUES(tagline),
                vote_average = VALUES(vote_average),
                vote_count = VALUES(vote_count),
                overview = VALUES(overview),
                poster_url = VALUES(poster_url),
                backdrop_url = VALUES(backdrop_url),
                release_date = VALUES(release_date),
                original_language = VALUES(original_language),
                age_rating = VALUES(age_rating),
                trailer_key = VALUES(trailer_key);
        """

        # Retrieve the age rating
        movie_title_age_rating = None
        us_movie_title_age_rating = None
        for release in movie_title_info['releases']['countries']:
            if release['iso_3166_1'] == 'FI':
                movie_title_age_rating = release['certification']
                break
            elif release['iso_3166_1'] == 'US' and release['certification'] != '':
                us_movie_title_age_rating = release['certification']

        if movie_title_age_rating == None:
            if us_movie_title_age_rating:
                movie_title_age_rating = us_movie_title_age_rating
            else:
                movie_title_age_rating = None


        # Retrieve the youtube trailer key
        movie_title_trailer_key = None
        for video in movie_title_info["videos"]["results"]:
            if video["site"] == "YouTube" and video["type"] == "Trailer":
                movie_title_trailer_key = video["key"]
                if video["official"] == True:
                    break

        # Generate params
        params = (
            movie_title_info.get('id'),
            movie_title_info.get('imdb_id'),
            'movie',
            movie_title_info.get('title'),
            movie_title_info.get('original_title'),
            movie_title_info.get('tagline'),
            movie_title_info.get('vote_average'),
            movie_title_info.get('vote_count'),
            movie_title_info.get('overview'),
            movie_title_info.get('poster_path'),
            movie_title_info.get('backdrop_path'),
            movie_title_info.get('runtime'),
            movie_title_info.get('release_date'),
            movie_title_info.get('original_language'),
            movie_title_age_rating,
            movie_title_trailer_key
        )
        title_id = query_mysql(query, params, fetch_last_row_id=True)

        # When updating the fetch_last_row_id returns a 0 for some reason so fetch the id seperately
        if title_id == 0:
            print("Need to fetch the title_id seperately")
            title_id_query = """
                SELECT title_id
                FROM titles
                WHERE tmdb_id = %s
            """
            title_id = query_mysql(title_id_query, (title_tmdb_id,))[0][0]

        # Store the title related images
        asyncio.create_task(store_title_images(movie_title_info.get('images'), title_id))

        # Handle genres using the seperate function
        add_or_update_genres_for_title(title_id, movie_title_info.get('genres', []))
        
        return title_id

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def add_or_update_tv_title(title_tmdb_id):
    try:
        # Get the data from tmdb
        tv_title_info = query_tmdb(f"/tv/{title_tmdb_id}", {
            "append_to_response": "external_ids,images,content_ratings,videos", 
            "include_image_language": "en,null"
        })

        # - - - TITLE - - - 
        # Insert the tv-series info into titles
        tv_title_query = """
            INSERT INTO titles (
                tmdb_id, 
                imdb_id, 
                type, 
                title_name, 
                title_name_original, 
                tagline, 
                vote_average, 
                vote_count, 
                overview, 
                poster_url, 
                backdrop_url, 
                movie_runtime, 
                release_date,
                original_language,
                age_rating,
                trailer_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                imdb_id = VALUES(imdb_id),
                title_name = VALUES(title_name),
                title_name_original = VALUES(title_name_original),
                tagline = VALUES(tagline),
                vote_average = VALUES(vote_average),
                vote_count = VALUES(vote_count),
                overview = VALUES(overview),
                poster_url = VALUES(poster_url),
                backdrop_url = VALUES(backdrop_url),
                release_date = VALUES(release_date),
                original_language = VALUES(original_language),
                age_rating = VALUES(age_rating),
                trailer_key = VALUES(trailer_key);
        """

        # Retrieve the age rating
        tv_title_age_rating = None
        us_tv_title_age_rating = None
        for release in tv_title_info['content_ratings']['results']:
            if release['iso_3166_1'] == 'FI':
                tv_title_age_rating = release['rating']
                break
            elif release['iso_3166_1'] == 'US' and release['rating'] != '':
                us_tv_title_age_rating = release['rating']

        if tv_title_age_rating == None:
            if us_tv_title_age_rating:
                tv_title_age_rating = us_tv_title_age_rating
            else:
                tv_title_age_rating = ""

        # Retrieve the youtube trailer key
        tv_title_trailer_key = None
        for video in tv_title_info["videos"]["results"]:
            if video["site"] == "YouTube" and video["type"] == "Trailer":
                tv_title_trailer_key = video["key"]
                if video["official"] == True:
                    break

        tv_title_params = (
            tv_title_info.get('id'),
            tv_title_info.get('external_ids', {}).get('imdb_id'),
            'tv',
            tv_title_info.get('name'),
            tv_title_info.get('original_name'),
            tv_title_info.get('tagline'),
            tv_title_info.get('vote_average'),
            tv_title_info.get('vote_count'),
            tv_title_info.get('overview'),
            tv_title_info.get('poster_path'),
            tv_title_info.get('backdrop_path'),
            None,   # there's no runtime since its tv
            tv_title_info.get('first_air_date'),
            tv_title_info.get('original_language'),
            tv_title_age_rating,
            tv_title_trailer_key
        )
        # Set id fetch to False since it often fails
        title_id = query_mysql(tv_title_query, tv_title_params, True)

        print("PÄÄSTIIN TÄNNE SAAKKA!")

        # When updating the fetch_last_row_id returns a 0 sometimes for some reason so fetch the id seperately
        if title_id == 0:
            print("Need to fetch the title_id seperately")
            title_id_query = """
                SELECT title_id
                FROM titles
                WHERE tmdb_id = %s
            """
            title_id = query_mysql(title_id_query, (title_tmdb_id,))[0][0]

        # Setup the title images to download in the background
        asyncio.create_task(store_title_images(tv_title_info.get('images'), title_id))

        # Handle genres using the function
        add_or_update_genres_for_title(title_id, tv_title_info.get('genres', []))

        # - - - SEASONS - - - 
        tv_seasons_params = []
        season_images_data = {
            "title_id": title_id,
            "seasons": []
        }

        for season in tv_title_info.get('seasons', []):
            if season.get('season_number') == 0:  # Skip season 0 (specials)
                continue
            
            # Add season data for MySQL insert
            tv_seasons_params.append((
                title_id,
                season.get('season_number'),
                season.get('name'),
                season.get('vote_average'),
                None,  # TMDB does not provide vote_count for seasons
                season.get('episode_count'),
                season.get('overview'),
                season.get('poster_path'),
            ))

            # Add season poster data for image storage
            if season.get("poster_path"):  # Only add if poster exists
                season_images_data = [
                    {"season_number": season["season_number"], "poster_path": season["poster_path"]}
                    for season in tv_title_info.get("seasons", [])
                    if season.get("poster_path")  # Only include valid posters
                ]

        # Insert into the database if there are valid seasons
        if tv_seasons_params:
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s)"] * len(tv_seasons_params))
            query = f"""
                INSERT INTO seasons (title_id, season_number, season_name, vote_average, vote_count, episode_count, overview, poster_url)
                VALUES {placeholders}
                ON DUPLICATE KEY UPDATE
                    season_name = VALUES(season_name),
                    vote_average = VALUES(vote_average),
                    vote_count = VALUES(vote_count),
                    episode_count = VALUES(episode_count),
                    overview = VALUES(overview),
                    poster_url = VALUES(poster_url)
            """
            flat_values = [item for sublist in tv_seasons_params for item in sublist]
            query_mysql(query, flat_values)

        # Setup the season images to download in the background
        asyncio.create_task(store_season_images(season_images_data, title_id))

        # - - - EPISODES - - - 
        # Fetch season IDs from the database
        season_id_query = "SELECT season_id, season_number FROM seasons WHERE title_id = %s"
        season_id_map = {row[1]: row[0] for row in query_mysql(season_id_query, (title_id,))}

        # Prepare list of tuples for bulk insertion
        tv_episodes_params = []
        episode_images_data = []

        for season in tv_title_info.get("seasons", []):
            season_number = season.get("season_number")
            season_id = season_id_map.get(season_number)  # Get correct season_id

            if season_id:
                season_info = query_tmdb(f"/tv/{title_tmdb_id}/season/{season_number}", {})

                for episode in season_info.get("episodes", []):
                    tv_episodes_params.append((
                        season_id,
                        title_id,
                        episode.get("episode_number"),
                        episode.get("name"),
                        episode.get("vote_average"),
                        episode.get("vote_count"),
                        episode.get("overview"),
                        episode.get("still_path"),
                        episode.get("air_date"),
                        episode.get("runtime")
                    ))

                    # Collect episode images for downloading
                    if episode.get("still_path"):
                        episode_images_data.append({
                            "season_number": season_number,
                            "episode_number": episode.get("episode_number"),
                            "still_path": episode.get("still_path")
                        })

        if tv_episodes_params:
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(tv_episodes_params))
            query = f"""
                INSERT INTO episodes (season_id, title_id, episode_number, episode_name, vote_average,
                                    vote_count, overview, still_url, air_date, runtime)
                VALUES {placeholders}
                ON DUPLICATE KEY UPDATE
                    episode_name = VALUES(episode_name),
                    vote_average = VALUES(vote_average),
                    vote_count = VALUES(vote_count),
                    overview = VALUES(overview),
                    still_url = VALUES(still_url),
                    air_date = VALUES(air_date),
                    runtime = VALUES(runtime)
            """
            flat_values = [item for sublist in tv_episodes_params for item in sublist]
            query_mysql(query, flat_values)

        # Setup the episode images to download in the background
        asyncio.create_task(store_episode_images(episode_images_data, title_id))

        # Finally return the title_id for later use
        return title_id

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/watch_list/add_user_title")
async def add_title(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key)

        # Check if the title already exists
        title_tmdb_id = data.get("title_tmdb_id")
        check_title_query = """
            SELECT title_id 
            FROM titles
            WHERE tmdb_id = %s
        """
        title_id = query_mysql(check_title_query, (title_tmdb_id,))


        # Add the title to the titles if it doesn't exist
        if not title_id:
            # Get and validate the type
            title_type = str(data.get("title_type")).lower()
            if title_type != "movie" and title_type != "tv":
                raise HTTPException(status_code=400, detail="Invalid title_type value!")
            
            # Based on type store the data
            if title_type == "movie":
                title_id = await add_or_update_movie_title(title_tmdb_id)
            elif title_type == "tv":
                title_id = await add_or_update_tv_title(title_tmdb_id)
            else:
                raise HTTPException(status_code=500, detail="Internal server error")
        else:
            # Get rid of unnescary arrays and wraps from the check_title_query.
            # The add movie title can give it properly so no need to do it for it.
            try:
                title_id = title_id[0][0]
            except:
                raise HTTPException(status_code=500, detail="Its this")

        # Add the link between the title and the user
        link_user_query = """
            INSERT INTO user_title_details (userID, title_id)
            VALUES(%s, %s)
        """
        query_mysql(link_user_query, (user_id, title_id))

        # If the query doesn't throw an error return success
        return {"success": True}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/watch_list/update_title_info")
async def update_title_info(data: dict):
    title_type = data.get("title_type")
    title_tmdb_id = data.get("title_tmdb_id")

    if not title_tmdb_id or not title_type:
        raise HTTPException(status_code=422, detail="Missing 'title_tmdb_id' or 'title_type'.")

    if title_type == "movie":
        await add_or_update_movie_title(title_tmdb_id)
    elif title_type == "tv":
        await add_or_update_tv_title(title_tmdb_id)
    else:
        raise HTTPException(status_code=422, detail="Invalid 'title_type'. Must be 'movie' or 'tv'.")

    return {"message": "Title information updated successfully."}


@app.post("/watch_list/update_title_images")
async def update_title_images(data: dict):

    # get title id from vue
    # from title id query the tmdb id and title type

    # Make a tmdb query for the tmdb images for the title 
    # Make a function that removes images for title
    # Use the made functions to query the images again in the bg

    return {"message": "Not implemented"}


@app.post("/watch_list/remove_user_title")
def remove_title(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key)

        # Get the TMDB title ID
        title_tmdb_id = data.get("title_tmdb_id")
        if not title_tmdb_id:
            raise HTTPException(status_code=400, detail="Missing title_tmdb_id")

        # Remove title from user's watch list
        remove_query = """
            DELETE user_title_details FROM user_title_details
            JOIN titles ON user_title_details.title_id = titles.title_id
            WHERE user_title_details.userID = %s AND titles.tmdb_id = %s
        """
        query_mysql(remove_query, (user_id, title_tmdb_id))

        # Remove related episodes from user's episode details
        remove_episodes_query = """
            DELETE user_episode_details
            FROM user_episode_details
            JOIN episodes ON user_episode_details.episode_id = episodes.episode_id
            WHERE user_episode_details.userID = %s AND episodes.title_id = (
                SELECT title_id FROM titles WHERE tmdb_id = %s
            )
        """
        query_mysql(remove_episodes_query, (user_id, title_tmdb_id))

        return {"success": True}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.post("/watch_list/save_user_title_notes")
def save_user_title_notes(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key)

        # Get the title ID and notes
        notes = data.get("notes")
        title_id = data.get("title_id")
        if not title_id:
            raise HTTPException(status_code=400, detail="Missing title_id")
        
        # Remove title from user's watch list
        save_notes_query = """
            UPDATE user_title_details
            SET notes = %s
            WHERE userID = %s AND title_id = %s
        """
        query_mysql(save_notes_query, (notes, user_id, title_id))

        return {"message": "Notes updated successfully!"}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    

@app.post("/watch_list/toggle_title_favourite")
def toggle_title_favourite(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key)

        # Get the title ID
        title_id = data.get("title_id")
        if not title_id:
            raise HTTPException(status_code=400, detail="Missing title_id")
        
        # Remove title from user's watch list
        save_notes_query = """
            INSERT INTO user_title_details (userID, title_id, favourite)
            VALUES (%s, %s, NOT favourite)
            ON DUPLICATE KEY UPDATE favourite = NOT favourite
        """
        query_mysql(save_notes_query, (user_id, title_id))

        return {"message": "Favourite status toggled successfully!"}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


def convert_season_or_episode_id_to_title_id(season_id=None, episode_id=None):
    if season_id:
        # Get the title_id from the season_id
        get_title_id_query = """
            SELECT title_id
            FROM seasons
            WHERE season_id = %s
        """
        result = query_mysql(get_title_id_query, (season_id,))
        title_id = result[0][0] if result else None
    elif episode_id:
        # Get the title_id from the episode_id
        get_title_id_query = """
            SELECT title_id
            FROM episodes
            WHERE episode_id = %s
        """
        result = query_mysql(get_title_id_query, (episode_id,))
        title_id = result[0][0] if result else None
    else:
        title_id = None

    return title_id

def keep_title_watch_count_up_to_date(user_id, title_id=None, season_id=None, episode_id=None):
    # If we don't have title_id, get it from season_id or episode_id
    if not title_id:
        title_id = convert_season_or_episode_id_to_title_id(season_id, episode_id)

    # If we have the title_id, proceed with checking episodes
    if title_id:
        check_all_watched_query = """
            SELECT COUNT(*) 
            FROM episodes e
            LEFT JOIN user_episode_details ued ON e.episode_id = ued.episode_id AND ued.userID = %s
            WHERE e.title_id = %s
            AND (ued.watch_count IS NULL OR ued.watch_count = 0)
        """
        result = query_mysql(check_all_watched_query, (user_id, title_id))
        
        # Check if all episodes are watched
        all_watched = result[0][0] == 0
        
        # Update the title's watch_count based on the check
        update_title_watch_count_query = """
            INSERT INTO user_title_details (userID, title_id, watch_count)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE watch_count = %s
        """
        query_mysql(update_title_watch_count_query, (user_id, title_id, 1 if all_watched else 0, 1 if all_watched else 0))

def get_updated_user_title_data(user_id: int, title_id: int):
    try:
        result = {}

        # Query for title's watch_count (for movie or TV show)
        title_query = """
            SELECT utd.watch_count
            FROM user_title_details utd
            WHERE utd.userID = %s AND utd.title_id = %s
        """
        title_info = query_mysql(title_query, (user_id, title_id))

        if title_info:
            result["title_id"] = title_id
            result["watch_count"] = title_info[0][0]

            # Query for episodes if it's a TV show
            episode_query = """
                SELECT e.episode_id, e.episode_name, ued.watch_count
                FROM episodes e
                LEFT JOIN user_episode_details ued 
                    ON e.episode_id = ued.episode_id AND ued.userID = %s
                WHERE e.title_id = %s
            """
            episode_info = query_mysql(episode_query, (user_id, title_id))
            result["episodes"] = [
                {
                    "episode_id": episode[0],
                    "episode_name": episode[1],
                    "watch_count": episode[2]
                }
                for episode in episode_info
            ]
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching updated data: {str(e)}")

@app.post("/watch_list/modify_title_watch_count")
def modify_title_watch_count(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validateSessionKey(session_key)

        # Get the type and chosen ID
        type = data.get("type")
        chosen_types_id = data.get("chosen_types_id")
        if not type:
            raise HTTPException(status_code=400, detail="Missing type.")
        if not chosen_types_id:
            raise HTTPException(status_code=400, detail="Missing chosen_types_id.")

        new_state = data.get("new_state")
        if new_state == "watched":
            watch_count = 1
        elif new_state == "unwatched":
            watch_count = 0
        else:
            raise HTTPException(status_code=400, detail="Invalid new_state. Valid values are 'watched' and 'unwatched'.")

        if type == "movie":
            title_query = """
                INSERT INTO user_title_details (userID, title_id, watch_count)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count)
            """
            query_mysql(title_query, (user_id, chosen_types_id, watch_count))

        elif type == "tv":
            episode_query = """
                INSERT INTO user_episode_details (userID, episode_id, watch_count)
                SELECT %s, episode_id, %s
                FROM episodes
                WHERE title_id = %s
                ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count);
            """
            query_mysql(episode_query, (user_id, watch_count, chosen_types_id))

            # Check and fix title watch_count
            keep_title_watch_count_up_to_date(user_id, title_id=chosen_types_id)

        elif type == "season":
            episode_query = """
                INSERT INTO user_episode_details (userID, episode_id, watch_count)
                SELECT %s, episode_id, %s
                FROM episodes
                WHERE season_id = %s
                ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count);
            """
            query_mysql(episode_query, (user_id, watch_count, chosen_types_id))

            # Check and fix title watch_count
            keep_title_watch_count_up_to_date(user_id, season_id=chosen_types_id)

        elif type == "episode":
            episode_query = """
                INSERT INTO user_episode_details (userID, episode_id, watch_count)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count)
            """
            query_mysql(episode_query, (user_id, chosen_types_id, watch_count))

            # Check and fix title watch_count
            keep_title_watch_count_up_to_date(user_id, episode_id=chosen_types_id)
        
        else:
            raise HTTPException(status_code=400, detail="Unknown type. Valid values are 'movie', 'tv', 'season', and 'episode'.")
        
        # Get the title_id for the updated data
        if type == "episode":
            title_id = convert_season_or_episode_id_to_title_id(episode_id=chosen_types_id)
        elif type == "season":
            title_id = convert_season_or_episode_id_to_title_id(season_id=chosen_types_id)
        else:
            title_id = chosen_types_id
        
        updated_data = get_updated_user_title_data(user_id, title_id)

        return {
            "message": "Watched state updated successfully!",
            "updated_data": updated_data
        }
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/watch_list/get_title_cards")
def get_title_cards(
    session_key: str = Query(...),
    title_count: int = None,
    # Optional sorting
    sort_by: str = None,
    # Optional filters
    title_type: str = Query(None, regex="^(movie|tv)$"),  
    watched: bool = None,
    favourite: bool = None,
    released: bool = None,
    started: bool = None,
):
    # Get user_id and validate session key
    user_id = validateSessionKey(session_key, False)

    # Base query
    get_titles_query = """
        SELECT 
            t.title_id, 
            t.title_name, 
            t.vote_average, 
            t.vote_count, 
            t.poster_url, 
            t.movie_runtime, 
            utd.watch_count, 
            t.type, 
            t.release_date,
            (SELECT COUNT(season_id) FROM seasons WHERE title_id = t.title_id) AS season_count,
            (SELECT COUNT(episode_id) FROM episodes WHERE title_id = t.title_id) AS episode_count,
            utd.favourite,
            GREATEST(COALESCE(utd.last_updated, '1970-01-01'), 
                    COALESCE(MAX(ued.last_updated), '1970-01-01')) AS latest_updated
        FROM 
            user_title_details utd
        JOIN 
            titles t ON utd.title_id = t.title_id
        LEFT JOIN 
            seasons s ON s.title_id = t.title_id
        LEFT JOIN 
            episodes e ON e.season_id = s.season_id
        LEFT JOIN 
            user_episode_details ued ON ued.userID = utd.userID AND ued.episode_id = e.episode_id
        WHERE 
            utd.userID = %s
    """

    query_params = [user_id]

    # Filter by category if provided
    if title_type:
        get_titles_query += " AND t.type = %s"
        query_params.append(title_type.lower())  # Ensure correct ENUM value

    # Filter by watched status if provided
    if watched is True:
        get_titles_query += " AND utd.watch_count >= 1"
    elif watched is False:
        get_titles_query += " AND utd.watch_count <= 0"

    # Filter by favourite status if provided
    if favourite is True:
        get_titles_query += " AND utd.favourite = TRUE"
    elif favourite is False:
        get_titles_query += " AND utd.favourite = FALSE"

    # Filter by release date if provided
    if released is True:
        get_titles_query += " AND t.release_date <= CURDATE()"
    elif released is False:
        get_titles_query += " AND t.release_date > CURDATE()"

    # Filter by whether the TV show has been started
    if started is True:
        get_titles_query += """
            AND EXISTS (
                SELECT 1 
                FROM user_episode_details ued 
                JOIN episodes e ON ued.episode_id = e.episode_id 
                WHERE ued.userID = utd.userID 
                AND e.title_id = t.title_id 
                AND ued.watch_count > 0
                LIMIT 1
            )
        """
    elif started is False:
        get_titles_query += """
            AND NOT EXISTS (
                SELECT 1 
                FROM user_episode_details ued 
                JOIN episodes e ON ued.episode_id = e.episode_id 
                WHERE ued.userID = utd.userID 
                AND e.title_id = t.title_id 
                AND ued.watch_count > 0
                LIMIT 1
            )
        """

    # Grouping clause
    get_titles_query += """
        GROUP BY
            t.title_id, 
            t.title_name, 
            t.vote_average, 
            t.vote_count, 
            t.poster_url, 
            t.movie_runtime, 
            utd.watch_count, 
            t.type, 
            t.release_date,
            utd.favourite
    """

    # Add sorting if provided
    if sort_by == "release_date":
        get_titles_query += " ORDER BY t.release_date DESC"
    elif sort_by == "last_watched":
        get_titles_query += " ORDER BY latest_updated DESC"
    else:
        get_titles_query += " ORDER BY t.vote_average DESC"

    # Add the limit
    if title_count is None:
        title_count = 12

    get_titles_query += " LIMIT %s"
    query_params.append(title_count)

    # Execute query
    results = query_mysql(get_titles_query, tuple(query_params))

    # Format results as objects with relevant fields
    formatted_results = []
    for row in results:
        # Base title data to which the rest is added to
        title_data = {
            "id": row[0],
            "name": row[1],
            "vote_average": row[2],
            "vote_count": row[3],
            "poster_url": row[4],
            "movie_runtime": row[5],
            "watch_count": row[6],
            "type": row[7],
            "release_date": row[8],
            "season_count": row[9],
            "episode_count": row[10],
            "is_favourite": row[11],
            "user_data_last_updated": row[12],
        }

        formatted_results.append(title_data)

    return {"titles": formatted_results}


def get_backdrop_count(title_id: int):
    # Path base
    base_path = "/fastapi-images"
    
    # Count of backdrops that exist
    count = 0

    # Loop through the backdrops from 1 to 5
    for i in range(1, 6):
        image_path = os.path.join(base_path, str(title_id), f"backdrop{i}.jpg")
        
        # Check if the image exists
        if os.path.exists(image_path):
            count += 1

    return count

@app.get("/watch_list/get_title_info")
def get_title_info(
    session_key: str = Query(...),
    title_id: int = Query(...),
):
    # Get user_id and validate session key
    user_id = validateSessionKey(session_key, False)

    # Check if user has added the title to their watchlist
    check_watchlist_query = """
        SELECT 1
        FROM user_title_details
        WHERE userID = %s AND title_id = %s
    """
    watchlist_exists = query_mysql(check_watchlist_query, (user_id, title_id))

    if watchlist_exists:
        # Base query for when title is in user's watchlist
        get_titles_query = """
            SELECT 
                t.title_id,
                t.tmdb_id, 
                t.imdb_id, 
                t.type, 
                t.title_name, 
                t.title_name_original, 
                t.tagline, 
                t.vote_average, 
                t.vote_count, 
                t.overview, 
                t.poster_url, 
                t.backdrop_url, 
                t.movie_runtime, 
                t.release_date,
                t.original_language,
                t.age_rating,
                t.trailer_key,
                t.last_updated,
                utd.watch_count,
                utd.notes,
                utd.favourite,
                utd.last_updated,
                GROUP_CONCAT(g.genre_name ORDER BY g.genre_name SEPARATOR ', ') AS genres
            FROM user_title_details utd
            JOIN titles t ON utd.title_id = t.title_id
            LEFT JOIN title_genres tg ON t.title_id = tg.title_id
            LEFT JOIN genres g ON tg.genre_id = g.genre_id
            WHERE utd.userID = %s AND utd.title_id = %s
            GROUP BY t.title_id, utd.watch_count, utd.notes, utd.last_updated;
        """
        title_query_results = query_mysql(get_titles_query, (user_id, title_id))[0]

        title_info = {
            "title_id": title_query_results[0],
            "tmdb_id": title_query_results[1],
            "imdb_id": title_query_results[2],
            "type": title_query_results[3],
            "name": title_query_results[4],
            "original_name": title_query_results[5],
            "tagline": title_query_results[6],
            "tmdb_vote_average": title_query_results[7],
            "tmdb_vote_count": title_query_results[8],
            "overview": title_query_results[9],
            "poster_url": title_query_results[10],
            "backdrop_url": title_query_results[11],
            "movie_runtime": title_query_results[12],
            "release_date": title_query_results[13],
            "original_language": title_query_results[14],
            "age_rating": title_query_results[15],
            "trailer_key": title_query_results[16],
            "title_info_last_updated": title_query_results[17],
            "user_title_watch_count": title_query_results[18],
            "user_title_notes": title_query_results[19],
            "user_title_favourite": title_query_results[20],
            "user_title_last_updated": title_query_results[21],
            "title_genres": title_query_results[22].split(", ") if title_query_results[22] else [],
            "backdrop_image_count": get_backdrop_count(title_query_results[0])
        }
    else:
        # Base query for when title is NOT in user's watchlist
        get_titles_query_not_on_list = """
            SELECT 
                t.title_id,
                t.tmdb_id, 
                t.imdb_id, 
                t.type, 
                t.title_name, 
                t.title_name_original, 
                t.tagline, 
                t.vote_average, 
                t.vote_count, 
                t.overview, 
                t.poster_url, 
                t.backdrop_url, 
                t.movie_runtime, 
                t.release_date,
                t.original_language,
                t.age_rating,
                t.trailer_key,
                t.last_updated,
                GROUP_CONCAT(g.genre_name ORDER BY g.genre_name SEPARATOR ', ') AS genres
            FROM titles t
            LEFT JOIN title_genres tg ON t.title_id = tg.title_id
            LEFT JOIN genres g ON tg.genre_id = g.genre_id
            WHERE t.title_id = %s
            GROUP BY t.title_id;
        """
        title_query_results_not_on_list = query_mysql(get_titles_query_not_on_list, (title_id,))[0]

        title_info = {
            "title_id": title_query_results_not_on_list[0],
            "tmdb_id": title_query_results_not_on_list[1],
            "imdb_id": title_query_results_not_on_list[2],
            "type": title_query_results_not_on_list[3],
            "name": title_query_results_not_on_list[4],
            "original_name": title_query_results_not_on_list[5],
            "tagline": title_query_results_not_on_list[6],
            "tmdb_vote_average": title_query_results_not_on_list[7],
            "tmdb_vote_count": title_query_results_not_on_list[8],
            "overview": title_query_results_not_on_list[9],
            "poster_url": title_query_results_not_on_list[10],
            "backdrop_url": title_query_results_not_on_list[11],
            "movie_runtime": title_query_results_not_on_list[12],
            "release_date": title_query_results_not_on_list[13],
            "original_language": title_query_results_not_on_list[14],
            "age_rating": title_query_results_not_on_list[15],
            "trailer_key": title_query_results_not_on_list[16],
            "title_info_last_updated": title_query_results_not_on_list[18],
            "user_title_watch_count": -1,
            "user_title_notes": None,
            "user_title_favourite": None,
            "user_title_last_updated": None,
            "title_genres": title_query_results_not_on_list[18].split(", ") if title_query_results_not_on_list[18] else [],
            "backdrop_image_count": get_backdrop_count(title_query_results_not_on_list[0])
        }

    # Get the seasons and episodes if it's a TV show
    if title_info["type"] == "tv":
        get_seasons_query = """
            SELECT season_id, season_number, season_name, vote_average, vote_count, episode_count, overview, poster_url
            FROM seasons
            WHERE title_id = %s
            ORDER BY CASE WHEN season_number = 0 THEN 999 ELSE season_number END
        """
        seasons = query_mysql(get_seasons_query, (title_id,))

        get_episodes_query = """
            SELECT 
                e.season_id, 
                e.episode_id, 
                e.episode_number, 
                e.episode_name, 
                e.vote_average, 
                e.vote_count, 
                e.overview, 
                e.still_url, 
                e.air_date, 
                e.runtime, 
                COALESCE(ued.watch_count, 0)
            FROM episodes e
            LEFT JOIN user_episode_details ued 
                ON e.episode_id = ued.episode_id 
                AND ued.userID = %s
            WHERE e.title_id = %s
            ORDER BY e.season_id, e.episode_number;
        """
        episodes = query_mysql(get_episodes_query, (user_id, title_id))

        # Organizing data into seasons with episodes
        season_map = {}
        for season in seasons:
            season_id = season[0]
            season_map[season_id] = {
                "season_id": season_id,
                "season_number": season[1],
                "season_name": season[2],
                "vote_average": season[3],
                "vote_count": season[4],
                "episode_count": season[5],
                "overview": season[6],
                "poster_url": season[7],
                "episodes": []
            }

        for episode in episodes:
            season_id = episode[0]
            if season_id in season_map:
                season_map[season_id]["episodes"].append({
                    "episode_id": episode[1],
                    "episode_number": episode[2],
                    "episode_name": episode[3],
                    "vote_average": episode[4],
                    "vote_count": episode[5],
                    "overview": episode[6],
                    "still_url": episode[7],
                    "air_date": episode[8],
                    "runtime": episode[9],
                    "watch_count": episode[10],
                })

        title_info["seasons"] = list(season_map.values())

    return {"title_info": title_info}


@app.get("/image/{image_path:path}")
@app.head("/image/{image_path:path}")  # HEAD for checking if image exists. Not really used anymore but doesn't hurt to have
async def get_image(image_path: str):
    # Define the full image path
    full_path = os.path.join("/fastapi-images", image_path)

    # Check if the image exists
    if os.path.exists(full_path):
        # If head just reutrn that it does
        if "head" in str(get_image.__name__).lower():  # HEAD request check
            return {"message": "Image exists"}
        # Else return the whole image
        return FileResponse(full_path)
    else:
        raise HTTPException(status_code=404, detail="Image doesn't exist.")



# Sort by options for future "/watch_list/list_titles":
    # Vote average (default)
    # Last watched
    # Alpabetical
    # Popularity (amount of votes)
    # Duration / Episode Count

# When updating watch count query the values for the title inside the updating endpoint and return them. 

# To add:
# production_companies (new table) and image function
