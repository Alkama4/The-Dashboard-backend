# Standard libraries
from datetime import timedelta
from typing import Literal
import pandas as pd
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql, validate_session_key

# Create the router object for this module
router = APIRouter()

@router.get("/balance_over_time")
def get_chart_balance_over_time(
    session_key: str = Query(None),
):
    try:
        # Validate the session key
        user_id = validate_session_key(session_key, False)

        # Fetch initial_balance from user_settings table
        initial_balance_query = """
            SELECT chart_balance_initial_value FROM user_settings WHERE user_id = %s
        """
        initial_balance_result = query_mysql(initial_balance_query, (user_id,))

        # Extract the initial balance
        initial_balance = initial_balance_result[0][0] if initial_balance_result else 0

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
        balance_result = query_mysql(balance_query, (user_id, initial_balance))

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


@router.get("/sum_by_month")
def get_chart_sum_by_month(
    session_key: str = Query(None),
):
    try:
        # Validate the session key
        user_id = validate_session_key(session_key, False)

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
                transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE 
                t.user_id = %s
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


@router.get("/categories_monthly")
def get_chart_categories_monthly(
    session_key: str = Query(None),
    direction: Literal['expense', 'income'] = Query(default='expense')
):
    try:
        user_id = validate_session_key(session_key, False)
        
        # Query for min and max dates across both directions
        date_range_query = """
            SELECT MIN(DATE_FORMAT(date, '%Y-%m')), MAX(DATE_FORMAT(date, '%Y-%m'))
            FROM transactions WHERE user_id = %s;
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
            JOIN transaction_items ti ON t.transaction_id = ti.transactionID
            WHERE t.user_id = %s AND t.direction = %s
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
