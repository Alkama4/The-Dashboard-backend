# Standard libraries
from datetime import datetime, date
from zoneinfo import ZoneInfo
from fastapi import HTTPException, Query, APIRouter
from typing import Literal
from datetime import timedelta
import pandas as pd

# Internal imports
from app.utils import validate_session_key_conn, aiomysql_conn_get, query_aiomysql, fetch_user_settings

# Create the router object for this module
router = APIRouter()


# ------------ Transactions ------------

@router.get("/transactions")
async def get_transactions(
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
    async with aiomysql_conn_get() as conn:

        # Validate the session key
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

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
                ORDER BY SUM(CASE WHEN t.direction = 'expense' THEN -ti.amount ELSE ti.amount END) {sort_order}, t.transaction_id DESC
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
                ORDER BY first_category.category {sort_order}, t.transaction_id DESC
                LIMIT %s OFFSET %s
            """
        else:
            transaction_query = f"""
                SELECT t.transaction_id
                FROM transactions t
                LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
                {where_clause}
                GROUP BY t.transaction_id
                ORDER BY {sort_by} {sort_order}, t.transaction_id DESC
                LIMIT %s OFFSET %s
            """

        # Get the limit from settings
        limit = await fetch_user_settings(conn, user_id, setting_name="transactions_load_limit")
        if not limit:
            limit = 25

        # Add limit and offset to parameters
        params.extend([limit, offset * limit])

        # Fetch transaction IDs
        transaction_ids = await query_aiomysql(conn, transaction_query, params, use_dictionary=False)
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
        transactions_items = await query_aiomysql(conn, items_query, items_params, use_dictionary=False)

        # Query for the total amount of transactions that match our filters and compare to the limit
        total_query = f"""
            SELECT COUNT(DISTINCT t.transaction_id)
            FROM transactions t
            LEFT JOIN transaction_items ti ON t.transaction_id = ti.transactionID
            {where_clause}
        """
        # Make a copy of the params list and remove the limit and offset
        params_for_total = params[:-2]
        total_count_result = await query_aiomysql(conn, total_query, params_for_total, use_dictionary=False)
        total_count = total_count_result[0][0]

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

        returned_count = offset * limit + len(transactions_dict)
        has_more = returned_count < total_count

        return {
            "transactions": list(transactions_dict.values()),
            "has_more": has_more,
            "total_count": total_count,
            "returned_count": returned_count,
            "offset": offset,
        }


@router.post("/transactions")
async def new_transaction(data: dict):
    async with aiomysql_conn_get() as conn:

        # Validate the session key
        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)

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
        transaction_query = """
            INSERT INTO transactions (direction, date, counterparty, notes, user_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        transaction_id = await query_aiomysql(conn, transaction_query, (direction, date, counterparty, notes, user_id), return_lastrowid=True, use_dictionary=False)

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
            await query_aiomysql(conn, item_query, (transaction_id, category_name, amount))

        return {"message": "Transaction created successfully!"}


@router.put("/transactions/{transaction_id}")
async def edit_transaction(transaction_id: int, data: dict):
    async with aiomysql_conn_get() as conn:

        # Validate the session key
        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)

        # Extract transaction data
        direction = data.get("direction")
        date = data.get("date")
        counterparty = data.get("counterparty")
        notes = data.get("notes")
        categories = data.get("categories", [])
        
        # Validate required fields
        if not all([transaction_id, direction, date, counterparty, categories]):
            raise HTTPException(status_code=400, detail="Missing required transaction fields.")

        # Check if the transaction exists and belongs to the user
        transaction_query = "SELECT * FROM transactions WHERE transaction_id = %s AND user_id = %s"
        transaction_result = await query_aiomysql(conn, transaction_query, (transaction_id, user_id), use_dictionary=False)
        if not transaction_result:
            raise HTTPException(status_code=403, detail="Transaction not found or not owned by the user.")

        # Update the transaction in the transactions table
        update_transaction_query = (
            "UPDATE transactions SET direction = %s, date = %s, counterparty = %s, notes = %s "
            "WHERE transaction_id = %s AND user_id = %s"
        )
        await query_aiomysql(conn, update_transaction_query, (direction, date, counterparty, notes, transaction_id, user_id))

        # Delete existing transaction items
        delete_items_query = "DELETE FROM transaction_items WHERE transactionID = %s"
        await query_aiomysql(conn, delete_items_query, (transaction_id,))

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
            await query_aiomysql(conn, item_query, (transaction_id, category_name, amount))

        return {"message": "Transaction edited successfully!"}


@router.delete("/transactions/{transaction_id}")
async def delete_transaction(transaction_id: int, data: dict):
    async with aiomysql_conn_get() as conn:

        # Validate the session key
        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)

        # Validate required fields
        if not transaction_id:
            raise HTTPException(status_code=400, detail="Transaction ID is required.")

        # Check if the transaction exists and belongs to the user
        transaction_query = "SELECT * FROM transactions WHERE transaction_id = %s AND user_id = %s"
        transaction_result = await query_aiomysql(conn, transaction_query, (transaction_id, user_id))
        if not transaction_result:
            raise HTTPException(status_code=403, detail="Transaction not found or not owned by the user.")

        # Delete the transaction
        delete_transaction_query = "DELETE FROM transactions WHERE transaction_id = %s AND user_id = %s"
        await query_aiomysql(conn, delete_transaction_query, (transaction_id, user_id))

        return {"message": "Transaction deleted successfully!"}


@router.get("/transactions/options/categories")
async def get_options(
    session_key: str = Query(None)
):
    async with aiomysql_conn_get() as conn:

        # Validate the session key
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

        # Counterparty query 
        counterparty_query = """
            SELECT counterparty, direction
            FROM transactions
            WHERE user_id = %s
            GROUP BY counterparty, direction
            ORDER BY COUNT(*) DESC;
        """
        counterpartyValuesObject = await query_aiomysql(conn, counterparty_query, (user_id,), use_dictionary=False)
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
        categoryValuesObject = await query_aiomysql(conn, category_query, (user_id,), use_dictionary=False)
        # Split into expense and income arrays based on the direction
        categoryExpense = [row[0] for row in categoryValuesObject if row[1] == "expense"]
        categoryIncome = [row[0] for row in categoryValuesObject if row[1] == "income"]

        return {"counterparty": {"expense": counterpartyExpense, "income": counterpartyIncome},
                "category": {"expense": categoryExpense, "income": categoryIncome}}


@router.get("/transactions/options/filters")
async def get_filters(
    session_key: str = Query(None)
):
    async with aiomysql_conn_get() as conn:

        # Validate the session key
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

        # Counterparty query with user_id filter
        counterparty_query = """
            SELECT counterparty, direction
            FROM transactions
            WHERE user_id = %s
            GROUP BY counterparty, direction
            ORDER BY COUNT(*) DESC;
        """
        counterpartyValuesObject = await query_aiomysql(conn, counterparty_query, (user_id,), use_dictionary=False)
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
        categoryValuesObject = await query_aiomysql(conn, category_query, (user_id,), use_dictionary=False)
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
        dateValues = await query_aiomysql(conn, date_query, (user_id,), use_dictionary=False)
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
        amountValues = await query_aiomysql(conn, amount_query, (user_id,), use_dictionary=False)
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


# ------------ Analytics ------------

@router.get("/analytics/stats/general")
async def analytics_get_general_stats(
    session_key: str = Query(None),
):
    async with aiomysql_conn_get() as conn:
        
        # Validate the session key
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

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
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE 
                t.user_id = %s
        """
        general_stats_result = await query_aiomysql(conn, general_stats_query, (user_id,), use_dictionary=False)

        # Prepare the response
        if general_stats_result:
            row = general_stats_result[0]
            result = {
                "transactionsLogged": row[0],
                "daysLogged": row[1],
                "avgLogsPerDay": float(row[2]) if row[2] is not None else 0,
                "totalExpenses": float(row[3]) if row[3] is not None else 0,
                "totalIncomes": float(row[4]) if row[4] is not None else 0,
            }
            return {"generalStats": result}

        return {"generalStats": {}}


@router.get("/analytics/stats/timespan")
async def analytics_get_timespan_stats(
    session_key: str = Query(...),
    start_date: str = Query(None),
    end_date: str = Query(None),
):
    async with aiomysql_conn_get() as conn:
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

        # Fetch start_date from DB if not given
        if not start_date:
            result = await query_aiomysql(
                conn,
                "SELECT MIN(t.date) FROM transactions t WHERE t.user_id = %s",
                (user_id,),
                use_dictionary=False
            )
            start_date_obj = result[0][0] or date.today()
        else:
            start_date_obj = date.fromisoformat(start_date)

        # Fetch end_date from DB if not given
        if not end_date:
            result = await query_aiomysql(
                conn,
                "SELECT MAX(t.date) FROM transactions t WHERE t.user_id = %s",
                (user_id,),
                use_dictionary=False
            )
            end_date_obj = result[0][0] or date.today()
        else:
            end_date_obj = date.fromisoformat(end_date)

        if start_date_obj > end_date_obj:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date.")

        days_in_period = (end_date_obj - start_date_obj).days + 1
        months_in_period = (end_date_obj.year - start_date_obj.year) * 12 + (end_date_obj.month - start_date_obj.month) + 1


        ###### Single value queries ######

        stats_query = """
            SELECT
                SUM(CASE WHEN t.direction = 'expense' THEN ti.amount ELSE 0 END) AS total_expenses
            FROM
                transactions t
            JOIN
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE
                t.user_id = %s AND t.date >= %s AND t.date <= %s
        """
        stats_result = await query_aiomysql(
            conn, stats_query, (user_id, start_date_obj, end_date_obj), use_dictionary=False
        )

        ratio_query = """
            SELECT
                COALESCE(SUM(CASE WHEN t.direction = 'income' THEN ti.amount ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN t.direction = 'expense' THEN ti.amount ELSE 0 END), 0)
            FROM
                transactions t
            JOIN
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE
                t.user_id = %s AND t.date >= %s AND t.date <= %s
        """
        ratio_result = await query_aiomysql(
            conn, ratio_query, (user_id, start_date_obj, end_date_obj), use_dictionary=False
        )

        # Calculate values
        total_expenses = stats_result[0][0] or 0 if stats_result else 0
        expenses_avg_day = float(total_expenses) / days_in_period if days_in_period else 0
        expenses_avg_week = float(total_expenses) / (days_in_period / 7) if days_in_period else 0
        expenses_avg_month = float(total_expenses) / months_in_period if months_in_period else 0

        total_incomes = float(ratio_result[0][0]) if ratio_result[0][0] is not None else 0
        total_expenses = float(ratio_result[0][1]) if ratio_result[0][1] is not None else 0
        income_expense_ratio = (total_incomes / total_expenses) if total_expenses != 0 else None
        net_total = total_incomes - total_expenses


        ###### Total sum value queries ######

        category_queries = {
            "expense": """
                SELECT
                    ti.category,
                    SUM(ti.amount) AS total_amount
                FROM
                    transactions t
                JOIN
                    transaction_items ti ON t.transaction_id = ti.transactionID
                WHERE
                    t.user_id = %s AND t.direction = 'expense' AND t.date >= %s AND t.date <= %s
                GROUP BY
                    ti.category
                ORDER BY
                    total_amount DESC;
            """,
            "income": """
                SELECT
                    ti.category,
                    SUM(ti.amount) AS total_amount
                FROM
                    transactions t
                JOIN
                    transaction_items ti ON t.transaction_id = ti.transactionID
                WHERE
                    t.user_id = %s AND t.direction = 'income' AND t.date >= %s AND t.date <= %s
                GROUP BY
                    ti.category
                ORDER BY
                    total_amount DESC;
            """
        }

        expense_category_result = await query_aiomysql(
            conn, category_queries["expense"], (user_id, start_date_obj, end_date_obj), use_dictionary=False
        )

        income_category_result = await query_aiomysql(
            conn, category_queries["income"], (user_id, start_date_obj, end_date_obj), use_dictionary=False
        )

        
        expenses_total_by_category = [
            {"category": row[0], "total_amount": float(row[1])} for row in expense_category_result
        ]

        expenses_avg_month_by_category = [
            {
                "category": row["category"],
                "avg_per_month": row["total_amount"] / months_in_period if months_in_period else 0
            }
            for row in expenses_total_by_category
        ]

        incomes_total_by_category = [
            {"category": row[0], "total_amount": float(row[1])} for row in income_category_result
        ]

        incomes_avg_month_by_category = [
            {
                "category": row["category"],
                "avg_per_month": row["total_amount"] / months_in_period if months_in_period else 0
            }
            for row in incomes_total_by_category
        ]

        return {
            "timespan": {
                "start_date": start_date_obj.isoformat(),
                "end_date": end_date_obj.isoformat(),
                "days_in_period": days_in_period,
                "months_in_period": months_in_period,
            },
            "stats": {
                "expenses_avg_day": expenses_avg_day,
                "expenses_avg_week": expenses_avg_week,
                "expenses_avg_month": expenses_avg_month,
                "expenses_total": total_expenses,
                "income_expense_ratio": income_expense_ratio,
                "net_total": net_total,
                "expense_categories_avg_month": expenses_avg_month_by_category,
                "expense_categories_total": expenses_total_by_category,
                "income_categories_total": incomes_total_by_category,
                "income_categories_avg_month": incomes_avg_month_by_category,
            }
        }



@router.get("/analytics/charts/{chart_type}")
async def get_charts(
    chart_type: Literal['balance_over_time', 'sum_by_month', 'categories_monthly'],
    session_key: str = Query(None),
    direction: Literal['expense', 'income'] = Query(default='expense')
):
    async with aiomysql_conn_get() as conn:
        
        # Validate the session key
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

        if chart_type == "balance_over_time":
            # Fetch initial_balance from user_settings table
            initial_balance_query = """
                SELECT chart_balance_initial_value FROM user_settings WHERE user_id = %s
            """
            initial_balance_result = await query_aiomysql(conn, initial_balance_query, (user_id,), use_dictionary=False)
            initial_balance = initial_balance_result[0][0] if initial_balance_result else 0

            # Query for the balance over time
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
                        transaction_items ti ON t.transaction_id = ti.transactionID
                    WHERE 
                        t.user_id = %s
                    GROUP BY 
                        t.date
                    ORDER BY 
                        t.date
                ) daily_balances
                JOIN 
                    (SELECT @running_balance := CAST(%s AS DECIMAL(10, 2))) r;
            """
            balance_result = await query_aiomysql(conn, balance_query, (user_id, initial_balance), use_dictionary=False)

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
                        missing_days = (current_date - previous_date).days - 1
                        for i in range(missing_days):
                            new_date = previous_date + timedelta(days=i + 1)
                            filled_balance_result.append({
                                "date": new_date,
                                "runningBalance": previous_balance
                            })

                    filled_balance_result.append({
                        "date": current_date,
                        "runningBalance": current_balance
                    })
                    previous_date = current_date
                    previous_balance = current_balance

                return {"balanceOverTime": filled_balance_result}
            return {"balanceOverTime": []}

        elif chart_type == "sum_by_month":
            # Query for the monthly sums of income, expense, and their total
            monthly_sum_query = """
                SELECT 
                    DATE_FORMAT(t.date, '%%Y-%%m') AS month,
                    SUM(CASE WHEN t.direction = 'income' AND t.date <= CURDATE() THEN ti.amount ELSE 0 END) AS past_income,
                    SUM(CASE WHEN t.direction = 'income' AND t.date > CURDATE() THEN ti.amount ELSE 0 END) AS upcoming_income,
                    SUM(CASE WHEN t.direction = 'expense' AND t.date <= CURDATE() THEN ti.amount * -1 ELSE 0 END) AS past_expense,
                    SUM(CASE WHEN t.direction = 'expense' AND t.date > CURDATE() THEN ti.amount * -1 ELSE 0 END) AS upcoming_expense,
                    SUM(CASE WHEN t.direction = 'income' THEN ti.amount
                            WHEN t.direction = 'expense' THEN ti.amount * -1
                            ELSE 0 END) AS net_total
                FROM
                    transactions t
                JOIN
                    transaction_items ti ON t.transaction_id = ti.transactionID
                WHERE
                    t.user_id = %s
                GROUP BY
                    month
                ORDER BY
                    month;
            """
            monthly_sum_result = await query_aiomysql(conn, monthly_sum_query, (user_id,), use_dictionary=False)

            current_month = datetime.utcnow().strftime('%Y-%m')

            def null_if_zero_and_out_of_range(value, month, is_past):
                if value == 0:
                    if is_past and month > current_month:
                        return None
                    if not is_past and month <= current_month:
                        return None
                return value

            if monthly_sum_result:
                formatted_result = []
                for row in monthly_sum_result:
                    month = row[0]
                    past_income = null_if_zero_and_out_of_range(float(row[1]), month, True)
                    upcoming_income = null_if_zero_and_out_of_range(float(row[2]), month, False)
                    past_expense = null_if_zero_and_out_of_range(float(row[3]), month, True)
                    upcoming_expense = null_if_zero_and_out_of_range(float(row[4]), month, False)
                    net_total = float(row[5])

                    formatted_result.append({
                        "month": month,
                        "past": {
                            "income": past_income,
                            "expense": past_expense,
                        },
                        "upcoming": {
                            "income": upcoming_income,
                            "expense": upcoming_expense,
                        },
                        "net_total": net_total
                    })
                return {"monthlySums": formatted_result}
            return {"monthlySums": []}

        elif chart_type == "categories_monthly":
            # Query for min and max dates across both directions
            date_range_query = """
                SELECT MIN(DATE_FORMAT(date, '%%Y-%%m')), MAX(DATE_FORMAT(date, '%%Y-%%m'))
                FROM transactions WHERE user_id = %s;
            """
            date_range_result = await query_aiomysql(conn, date_range_query, (user_id,), use_dictionary=False)
            first_month, last_month = date_range_result[0] if date_range_result else (None, None)

            if not first_month or not last_month:
                return {"monthlyCategoryExpenses": []}

            query = """
                SELECT 
                    DATE_FORMAT(t.date, '%%Y-%%m') AS month,
                    ti.category,
                    SUM(ti.amount) AS total_expense
                FROM transactions t
                JOIN transaction_items ti ON t.transaction_id = ti.transactionID
                WHERE t.user_id = %s AND t.direction = %s
                GROUP BY month, ti.category
                ORDER BY month, ti.category;
            """
            results = await query_aiomysql(conn, query, (user_id, direction), use_dictionary=False)

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
