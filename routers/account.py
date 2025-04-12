# Standard libraries
import random
import string
from datetime import datetime, timedelta
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql, validate_session_key

# Create the router object for this module
router = APIRouter()

@router.post("/login")
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
            WHERE user_id = (
                SELECT user_id 
                FROM sessions 
                WHERE session_id = %s AND expires_at > NOW()
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
    query = "SELECT user_id, password FROM users WHERE username = %s"
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
    user_id = user[0][0]  # Get user_id from the query result
    insert_query = """
        INSERT INTO sessions (session_id, user_id, expires_at) 
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


@router.get("/get_login_status")
def get_login_status(
    session_key: str = Query(...)
):
    # Check if the session key exists and join it with the user table to get the username
    query = """
    SELECT username 
    FROM users 
    WHERE user_id = (
        SELECT user_id 
        FROM sessions 
        WHERE session_id = %s AND expires_at > NOW()
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
    

@router.post("/logout")
def logout(
    sessionKey: str = Query(...)
):
    # Delete the session key from the sessions table
    query = "DELETE FROM sessions WHERE session_id = %s"
    query_mysql(query, (sessionKey,))
    return {
        "logOutSuccess": True,
    }


@router.post("/create")
def create_account(data: dict):

    username = data.get("username")
    password = data.get("password")
    if (username and password):
        # Validate lengths
        if len(password) < 4:
            raise HTTPException(status_code=400, detail="The password is too short. Minimum allowed length is 8.")
        elif len(password) > 256:
            raise HTTPException(status_code=400, detail="The password is too long. Maxiumum allowed length is 256.")

        if len(username) < 4:
            raise HTTPException(status_code=400, detail="The username is too short. Minimum allowed length is 4.")
        elif len(username) > 128:
            raise HTTPException(status_code=400, detail="The username is too long. Maxiumum allowed length is 128.")

        check_for_same_name_query = """
            SELECT user_id 
            FROM users
            WHERE username = %s
        """
        check_for_same_name_params = (username,)
        check_for_same_name_result = query_mysql(check_for_same_name_query, check_for_same_name_params)

        if check_for_same_name_result:
            raise HTTPException(status_code=400, detail="The username is already taken.")
        
        else:
            # Create user in users table
            create_user_query = """
                INSERT INTO users (username, password)
                VALUES (%s, %s);
            """
            create_user_params = (username, password)
            user_id = query_mysql(create_user_query, create_user_params, True)
            
            # Initialize user settings
            create_settings_query = """
                INSERT INTO user_settings (user_id)
                VALUES (%s);
            """
            create_settings_params = (user_id,)
            query_mysql(create_settings_query, create_settings_params)

            return {"message": "Account created successfully!"}
    
    else:
        raise HTTPException(status_code=400, detail="Missing username or password.")


@router.post("/change_password")
def change_password(data: dict):
    try:
        user_id = validate_session_key(data.get("session_key"), True)
        password_old = data.get("password_old")
        password_new = data.get("password_new")
        if (password_old and password_new):

            if (len(password_new) < 6):
                raise HTTPException(status_code=400, detail="The password is too short. Minimum allowed length is 6.")
            elif len(password_new > 256):
                raise HTTPException(status_code=400, detail="The password is too long. Maxiumum allowed length is 256.")

            change_password_query = """
                UPDATE users
                SET password = %s
                WHERE user_id = %s AND password = %s;
            """
            change_password_params = (password_new, user_id, password_old)
            affected_rows = query_mysql(change_password_query, change_password_params)
            if affected_rows == 0:
                raise HTTPException(status_code=400, detail="Invalid password!")

            return {"message": "Account created successfully!"}
        
        else:
            raise HTTPException(status_code=400, detail="Missing password.")

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# The password should be asked twice etc on the front end but I guess on the backend we should just do the thing.
@router.post("/delete")
def delete_account(data: dict):
    try:
        user_id = validate_session_key(data.get("session_key"), True)
        password = data.get("password")

        if password:
            delete_user_query = """
                DELETE FROM users 
                WHERE user_id = %s AND password = %s;
            """
            delete_user_params = (user_id, password)
            affected_rows = query_mysql(delete_user_query, delete_user_params)

            if affected_rows == 0:
                raise HTTPException(status_code=400, detail="Incorrect password.")

            return {"message": "Your account and all the data related to it has been successfully deleted!"}

        raise HTTPException(status_code=400, detail="Missing password.")

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# Valid settings so that adding more is simpler
VALID_SETTINGS = [
    "transactions_load_limit",
    "chart_balance_initial_value",
    "list_all_titles_load_limit"
]

@router.get("/get_settings")
def get_settings(session_key: str):
    
    # Validate the sessionkey and get the user_id
    user_id = validate_session_key(session_key, False)

    query = f"""
        SELECT {', '.join(VALID_SETTINGS)}
        FROM user_settings
        WHERE user_id = %s;
    """
    result = query_mysql(query, (user_id,))[0]

    return {setting: result[i] for i, setting in enumerate(VALID_SETTINGS)}


@router.post("/update_settings")
def update_settings(data: dict):
    # Validate the sessionkey and get the user_id
    session_key = data.get("session_key")
    user_id = validate_session_key(session_key, True)

    changed_settings = data.get("changed_settings")

    if not changed_settings:
        return {"message": "No settings to update"}

    # Prepare the SET clause and values for the update query
    set_clause = []
    values = []

    for setting in changed_settings:
        setting_name = setting["setting"]
        value = setting["value"]
        
        # Ensure that the setting name matches the columns in the database
        if setting_name in VALID_SETTINGS:
            set_clause.append(f"{setting_name} = %s")
            values.append(value)

    # If there are no valid settings to update
    if not set_clause:
        return {"message": "No valid settings to update"}

    # Construct the query
    # Need to use join since the %s can't be used for column names
    query = f"""
        UPDATE user_settings
        SET {', '.join(set_clause)}
        WHERE user_id = %s;
    """

    # Add user_id to the values to pass into the query
    values.append(user_id)

    # Execute the query
    query_mysql(query, tuple(values))

    return {"message": "Settings updated successfully"}
