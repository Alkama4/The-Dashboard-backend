# Standard libraries
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql, validate_session_key

# Create the router object for this module
router = APIRouter()

@router.post("/new_transaction")
def new_transaction(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validate_session_key(session_key, True)

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
            "INSERT INTO transactions (direction, date, counterparty, notes, user_id) "
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


@router.post("/edit_transaction")
def edit_transaction(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validate_session_key(session_key, True)

        # Extract transaction data
        transaction_id = data.get("transaction_id")
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
        transaction_query = "SELECT * FROM transactions WHERE transaction_id = %s AND user_id = %s"
        transaction_result = query_mysql(transaction_query, (transaction_id, user_id))
        if not transaction_result:
            raise HTTPException(status_code=403, detail="Transaction not found or not owned by the user.")

        # Update the transaction in the transactions table
        update_transaction_query = (
            "UPDATE transactions SET direction = %s, date = %s, counterparty = %s, notes = %s "
            "WHERE transaction_id = %s AND user_id = %s"
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
@router.post("/delete_transaction")
def delete_transaction(data: dict):
    try:
        # Validate the session key
        session_key = data.get("session_key")
        user_id = validate_session_key(session_key, True)

        # Extract transaction data
        transaction_id = data.get("transaction_id")

        # Validate required fields
        if not transaction_id:
            raise HTTPException(status_code=400, detail="Transaction ID is required.")

        # Check if the transaction exists and belongs to the user
        transaction_query = "SELECT * FROM transactions WHERE transaction_id = %s AND user_id = %s"
        transaction_result = query_mysql(transaction_query, (transaction_id, user_id))
        if not transaction_result:
            raise HTTPException(status_code=403, detail="Transaction not found or not owned by the user.")

        # Delete associated transaction items
        delete_items_query = "DELETE FROM transaction_items WHERE transactionID = %s"
        query_mysql(delete_items_query, (transaction_id,))

        # Delete the transaction
        delete_transaction_query = "DELETE FROM transactions WHERE transaction_id = %s AND user_id = %s"
        query_mysql(delete_transaction_query, (transaction_id, user_id))

        return {"deleteTransactionSuccess": True}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/get_transactions")
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
    offset: int = Query(0, ge=0),
    session_key: str = Query(None)
):
    # Validate the session key
    user_id = validate_session_key(session_key, False)

    # Initialize filters and parameters
    filters = ["t.user_id = %s"]
    params = [user_id]

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
            t.transaction_id IN (
                SELECT ti.transactionID
                FROM transaction_items ti
                LEFT JOIN transactions t2 ON ti.transactionID = t2.transaction_id
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
            SELECT t.transaction_id
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
            {where_clause}
            GROUP BY t.transaction_id
            ORDER BY SUM(CASE WHEN t.direction = 'expense' THEN -ti.amount ELSE ti.amount END) {sort_order}
            LIMIT %s OFFSET %s
        """
    elif sort_by == "category":
        transaction_query = f"""
            SELECT t.transaction_id
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
            LEFT JOIN (
                SELECT transactionID,
                    GROUP_CONCAT(category ORDER BY item_id) AS category
                FROM transaction_items
                GROUP BY transactionID
            ) AS first_category ON t.transaction_id = first_category.transactionID
            {where_clause}
            GROUP BY t.transaction_id
            ORDER BY first_category.category {sort_order}
            LIMIT %s OFFSET %s
        """
    else:
        transaction_query = f"""
            SELECT t.transaction_id
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
            {where_clause}
            GROUP BY t.transaction_id
            ORDER BY {sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """

    # Get the limit from settings
    spendings_limit_query = """
        SELECT transactions_load_limit FROM user_settings WHERE user_id = %s
    """
    spendings_limit_result = query_mysql(spendings_limit_query, (user_id,))
    limit = spendings_limit_result[0][0] if spendings_limit_result else 25

    # Add limit and offset to parameters
    params.extend([limit, offset * limit])

    # Fetch transaction IDs
    transaction_ids = query_mysql(transaction_query, params)
    if not transaction_ids:
        return {"transactions": []}

    # Extract transaction IDs
    transaction_ids_list = [t[0] for t in transaction_ids]

    # Fetch transaction items
    placeholders = ','.join(['%s'] * len(transaction_ids_list))
    items_query = f"""
        SELECT t.transaction_id, t.direction, t.date, t.counterparty, t.notes, ti.category, ti.amount
        FROM transactions t
        LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
        WHERE t.transaction_id IN ({placeholders})
        ORDER BY FIELD(t.transaction_id, {','.join(['%s'] * len(transaction_ids_list))})
    """
    items_params = transaction_ids_list + transaction_ids_list
    transactions_items = query_mysql(items_query, items_params)

    # Query for the total amount of transactions that match our filters and compare to the limit
    total_query = f"""
        SELECT COUNT(DISTINCT t.transaction_id)
        FROM transactions t
        LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
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
        transaction_id = transaction[0]
        if transaction_id not in transactions_dict:
            transactions_dict[transaction_id] = {
                "transaction_id": transaction_id,
                "direction": transaction[1],
                "date": transaction[2],
                "counterparty": transaction[3],
                "notes": transaction[4],
                "categories": [],
                "amount_sum": 0,
            }
        transactions_dict[transaction_id]["categories"].append({
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


@router.get("/get_options")
def get_options(
    session_key: str = Query(None)
):
    # Validate the session key
    user_id = validate_session_key(session_key, False)

    # Counterparty query 
    counterparty_query = """
        SELECT counterparty, direction
        FROM transactions
        WHERE user_id = %s
        GROUP BY counterparty, direction
        ORDER BY COUNT(*) DESC;
    """
    counterpartyValuesObject = query_mysql(counterparty_query, (user_id,))
    # Split into expense and income arrays based on the direction
    counterpartyExpense = [row[0] for row in counterpartyValuesObject if row[1] == "expense"]
    counterpartyIncome = [row[0] for row in counterpartyValuesObject if row[1] == "income"]

    # Category query with user_id filter
    category_query = """
        SELECT ti.category, t.direction
        FROM transaction_items ti
        JOIN transactions t ON ti.transactionID = t.transaction_id
        WHERE t.user_id = %s
        GROUP BY ti.category, t.direction
        ORDER BY COUNT(*) DESC;
    """
    categoryValuesObject = query_mysql(category_query, (user_id,))
    # Split into expense and income arrays based on the direction
    categoryExpense = [row[0] for row in categoryValuesObject if row[1] == "expense"]
    categoryIncome = [row[0] for row in categoryValuesObject if row[1] == "income"]

    return {"counterparty": {"expense": counterpartyExpense, "income": counterpartyIncome},
            "category": {"expense": categoryExpense, "income": categoryIncome}}


@router.get("/get_filters")
def get_filters(
    session_key: str = Query(None)
):

    # Validate the session key
    user_id = validate_session_key(session_key, False)

    try:
        # Counterparty query with user_id filter
        counterparty_query = """
            SELECT counterparty, direction
            FROM transactions
            WHERE user_id = %s
            GROUP BY counterparty, direction
            ORDER BY COUNT(*) DESC;
        """
        counterpartyValuesObject = query_mysql(counterparty_query, (user_id,))
        # Split into expense and income arrays based on the direction
        counterpartyExpense = [row[0] for row in counterpartyValuesObject if row[1] == "expense"]
        counterpartyIncome = [row[0] for row in counterpartyValuesObject if row[1] == "income"]

        # Category query with user_id filter
        category_query = """
            SELECT ti.category, t.direction
            FROM transaction_items ti
            JOIN transactions t ON ti.transactionID = t.transaction_id
            WHERE t.user_id = %s
            GROUP BY ti.category, t.direction
            ORDER BY COUNT(*) DESC;
        """
        categoryValuesObject = query_mysql(category_query, (user_id,))
        # Split into expense and income arrays based on the direction
        categoryExpense = [row[0] for row in categoryValuesObject if row[1] == "expense"]
        categoryIncome = [row[0] for row in categoryValuesObject if row[1] == "income"]

        # Query for min and max dates as UNIX timestamps
        date_query = """
            SELECT 
                UNIX_TIMESTAMP(MIN(date)) AS minDate, 
                UNIX_TIMESTAMP(MAX(date)) AS maxDate
            FROM transactions
            WHERE user_id = %s;
        """
        dateValues = query_mysql(date_query, (user_id,))
        minDate = dateValues[0][0]
        maxDate = dateValues[0][1]

        # Query for max and min amounts, adjusting for direction, with user_id filter
        amount_query = """
            SELECT MAX(adjusted_amount) AS maxAmount, MIN(adjusted_amount) AS minAmount
            FROM (
                SELECT t.transaction_id, 
                    SUM(CASE 
                        WHEN t.direction = 'expense' THEN -ti.amount
                        WHEN t.direction = 'income' THEN ti.amount
                    END) AS adjusted_amount
                FROM transaction_items ti
                JOIN transactions t ON ti.transactionID = t.transaction_id
                WHERE t.user_id = %s
                GROUP BY t.transaction_id
            ) AS transaction_totals;
        """
        amountValues = query_mysql(amount_query, (user_id,))
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
