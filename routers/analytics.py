# Standard libraries
import calendar
from datetime import date
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql, validate_session_key

# Create the router object for this module
router = APIRouter()

@router.get("/get_general_stats")
def analytics_get_general_stats(
    session_key: str = Query(None),
):
    try:
        # Validate the session key
        user_id = validate_session_key(session_key, False)

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
        general_stats_result = query_mysql(general_stats_query, (user_id,))

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

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/get_stats_for_timespan")
def analytics_get_last_timespan_stats(
    session_key: str = Query(None),
    timespan: str = Query(None),
):
    try:
        # Validate the session key
        user_id = validate_session_key(session_key, False)

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
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE 
                t.user_id = %s AND {date_condition}
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
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE 
                t.user_id = %s AND {date_condition};
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
        #         transaction_items ti ON t.transaction_id = ti.transactionID
        #     WHERE 
        #         t.user_id = %s AND t.direction = 'expense' AND {date_condition}
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
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE 
                t.user_id = %s AND t.direction = 'expense' AND {date_condition}
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
