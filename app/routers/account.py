# Standard libraries
import random
import string
from datetime import datetime, timedelta
from fastapi import HTTPException, Query, APIRouter, UploadFile, File, Form
import uuid
import os
from io import BytesIO
from PIL import Image

# Internal imports
from app.utils import aiomysql_conn_get, query_aiomysql, validate_session_key_conn

# Create the router object for this module
router = APIRouter()

@router.post("/login")
async def login(data: dict):
    async with aiomysql_conn_get() as conn:

        username = data.get('username')
        password = data.get('password')
        previous_session_key = data.get('previous_session_key')

        # Check if the user is already logged in
        if previous_session_key:
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
            result = await query_aiomysql(conn, query, (previous_session_key,), use_dictionary=False)
            if result:
                logged_in_username = result[0][0]  # Extract the username from the query result
                if logged_in_username.lower() == username.lower():
                    return {
                        "loginStatus": "warning", 
                        "statusMessage": "Already logged in.",
                        "sessionKey": previous_session_key, 
                    }

        # Check if the user exists in the database and query the password
        query = "SELECT user_id, password FROM users WHERE username = %s"
        user = await query_aiomysql(conn, query, (username,), use_dictionary=False)
        
        # Basic password check (plaintext)
        if not user or user[0][1] != password:  
            return {
                "loginStatus": "error",
                "statusMessage": "Invalid username or password."
                }   

        # Generate a session key
        session_key = ''.join(random.choices(string.ascii_letters + string.digits, k=36))

        # Set the session expiration time
        expiration_time = datetime.now() + timedelta(days=90)   # Could be less, but is annoying and unnescary for the scope.

        # Insert the session key into the sessions table
        user_id = user[0][0]  # Get user_id from the query result
        insert_query = """
            INSERT INTO sessions (session_id, user_id, expires_at) 
            VALUES (%s, %s, %s)
        """
        await query_aiomysql(conn, insert_query, (session_key, user_id, expiration_time))

        # Lastly delete expired sessions from the sessions table
        expired_query = """
            DELETE FROM sessions WHERE expires_at <= NOW();
        """
        await query_aiomysql(conn, expired_query)

        # Return the session key to the client
        return {
            "message": "Logged in successfully!",
            "sessionKey": session_key,
            "username": username,
        }


@router.post("/logout")
async def logout(data: dict):
    async with aiomysql_conn_get() as conn:

        session_key = data.get("session_key")
        if not session_key:
            raise HTTPException(status_code=400, detail="Missing parameter: session_key.")

        # Delete the session key from the sessions table
        query = "DELETE FROM sessions WHERE session_id = %s"
        await query_aiomysql(conn, query, (session_key,))
        return {
            "message": "Logged out successfully!",
        }


@router.post("/")
async def create_account(data: dict):
    async with aiomysql_conn_get() as conn:

        username = data.get("username")
        password = data.get("password")

        if (not username or not password):
            raise HTTPException(status_code=400, detail="Missing username or password.")

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
        check_for_same_name_result = await query_aiomysql(conn, check_for_same_name_query, (username,), use_dictionary=False)

        if check_for_same_name_result:
            raise HTTPException(status_code=400, detail="The username is already taken.")
        
        else:
            # Create user in users table
            create_user_query = """
                INSERT INTO users (username, password)
                VALUES (%s, %s);
            """
            create_user_params = (username, password)
            user_id = await query_aiomysql(conn, create_user_query, create_user_params, return_lastrowid=True)
            
            # Initialize user settings
            create_settings_query = """
                INSERT INTO user_settings (user_id)
                VALUES (%s);
            """
            create_settings_params = (user_id,)
            await query_aiomysql(conn, create_settings_query, create_settings_params)

            return {"message": "Account created successfully!"}
        

# Used to be get login status
@router.get("/session")
async def get_session_details(
    session_key: str = Query(...)
):
    async with aiomysql_conn_get() as conn:
        # Check if the session key exists and join it with
        # the user table to get the username.
        query = """
            SELECT username 
            FROM users 
            WHERE user_id = (
                SELECT user_id 
                FROM sessions 
                WHERE session_id = %s AND expires_at > NOW()
            );
        """
        result = await query_aiomysql(conn, query, (session_key,), use_dictionary=False)

        if result:
            return {
                "active": True, 
                "username": result[0][0],
            }

        else:
            return {
                "active": False,
                "username": None,
            }


# The password should be asked twice etc on the front end,
# but I guess on the backend we should just perform the deletion.
@router.delete("/")
async def delete_account(data: dict):
    async with aiomysql_conn_get() as conn:

        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)
        password = data.get("password")

        if password:
            delete_user_query = """
                DELETE FROM users 
                WHERE user_id = %s AND password = %s;
            """
            delete_user_params = (user_id, password)
            affected_rows = await query_aiomysql(conn, delete_user_query, delete_user_params, return_rowcount=True)

            if affected_rows == 0:
                raise HTTPException(status_code=400, detail="Incorrect password.")

            return {"message": "Your account and all the data related to it has been successfully deleted!"}

        raise HTTPException(status_code=400, detail="Missing password.")


@router.put("/password")
async def change_password(data: dict):
    async with aiomysql_conn_get() as conn:

        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)
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
            affected_rows = await query_aiomysql(conn, change_password_query, change_password_params, return_rowcount=True)
            if affected_rows == 0:
                raise HTTPException(status_code=400, detail="Invalid password!")

            return {"message": "Account created successfully!"}
        
        else:
            raise HTTPException(status_code=400, detail="Missing password.")


# List here to make adding and modifying process simpler and more unified
VALID_SETTINGS = [
    "transactions_load_limit",
    "chart_balance_initial_value",
    "list_all_titles_load_limit"
]

@router.get("/settings")
async def get_settings(session_key: str):
    async with aiomysql_conn_get() as conn:
    
        # Validate the sessionkey and get the user_id
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

        query = f"""
            SELECT {', '.join(VALID_SETTINGS)}
            FROM user_settings
            WHERE user_id = %s;
        """
        result = await query_aiomysql(conn, query, (user_id,), use_dictionary=False)
        setting_values = result[0]

        return {setting: setting_values[i] for i, setting in enumerate(VALID_SETTINGS)}


@router.put("/settings")
async def update_settings(data: dict):
    async with aiomysql_conn_get() as conn:

        # Validate the sessionkey and get the user_id
        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)

        updated_settings = data.get("updated_settings")
        if not updated_settings:
            return {"message": "No settings to update."}

        # Prepare the SET clause and values for the update query
        set_clause = []
        values = []

        for setting in updated_settings:
            setting_name = setting["setting"]
            value = setting["value"]
            
            # Ensure that the setting name matches the columns in the database
            if setting_name in VALID_SETTINGS:
                set_clause.append(f"{setting_name} = %s")
                values.append(value)

        # If there are no valid settings to update
        if not set_clause:
            return {"message": "No valid settings to update."}

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
        await query_aiomysql(conn, query, tuple(values))

        return {"message": "Settings updated successfully!"}


@router.post("/external_service_links")
async def external_service_links(
    session_key: str = Form(...),
    name: str = Form(...),
    link: str = Form(...),
    description: str | None = Form(None),
    image: UploadFile | None = File(None),
):
    async with aiomysql_conn_get() as conn:
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=True)

        MAX_SIZE = 500  # max width or height in pixels
        
        external_image_path = None
        if image:
            ext = os.path.splitext(image.filename)[1].lower()
            if ext not in {".png", ".jpeg", ".jpg", ".svg"}:
                raise HTTPException(status_code=400, detail="Unsupported file type")

            contents = await image.read()
            image_uuid = uuid.uuid4()
            external_media_dir = "/service-images"
            internal_media_dir = f"/fastapi-media{external_media_dir}"
            os.makedirs(internal_media_dir, exist_ok=True)
            external_image_path = os.path.join(external_media_dir, f"{image_uuid}{ext}")
            internal_image_path = os.path.join(internal_media_dir, f"{image_uuid}{ext}")

            if ext != ".svg":
                img = Image.open(BytesIO(contents))
                img.thumbnail((MAX_SIZE, MAX_SIZE))  # resizes preserving aspect ratio
                img.save(internal_image_path)  # optionally choose a standard format like PNG
            else:
                # Save SVG as-is
                with open(internal_image_path, "wb") as f:
                    f.write(contents)
                    
        create_entry_query = """
            INSERT INTO user_external_service_links
            (user_id, name, link, description, image_path)
            VALUES (%s, %s, %s, %s, %s);
        """
        create_entry_params = (user_id, name, link, description, external_image_path)
        await query_aiomysql(conn, create_entry_query, create_entry_params)

        return {"message": f'External service link "{name}" created successfully!'}
    

@router.put("/external_service_links/{link_id}")
async def update_external_service_link(
    link_id: int,
    session_key: str = Form(...),
    name: str = Form(...),
    link: str = Form(...),
    description: str | None = Form(None),
    image: UploadFile | None = File(None),
    remove_image: bool = Form(False),
):
    async with aiomysql_conn_get() as conn:
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=True)

        # Fetch existing record
        existing_rows = await query_aiomysql(
            conn,
            "SELECT image_path FROM user_external_service_links WHERE id = %s AND user_id = %s LIMIT 1",
            (link_id, user_id),
        )
        if not existing_rows:
            raise HTTPException(status_code=404, detail="Service link not found")

        old_external_path = existing_rows[0]["image_path"]
        old_internal_path = os.path.join("/fastapi-media", old_external_path) if old_external_path else None
        new_external_path = old_external_path  # default to existing

        if remove_image:
            if old_internal_path and os.path.exists(old_internal_path):
                os.remove(old_internal_path)
            new_external_path = None

        elif image:
            # Delete old image if exists
            if old_internal_path and os.path.exists(old_internal_path):
                os.remove(old_internal_path)

            ext = os.path.splitext(image.filename)[1].lower()
            if ext not in {".png", ".jpeg", ".jpg", ".svg"}:
                raise HTTPException(status_code=400, detail="Unsupported file type")

            contents = await image.read()
            image_uuid = uuid.uuid4()
            external_media_dir = "/service-images"
            internal_media_dir = f"/fastapi-media{external_media_dir}"
            os.makedirs(internal_media_dir, exist_ok=True)

            new_external_path = os.path.join(external_media_dir, f"{image_uuid}{ext}")
            internal_path = os.path.join(internal_media_dir, f"{image_uuid}{ext}")

            if ext != ".svg":
                img = Image.open(BytesIO(contents))
                img.thumbnail((500, 500))
                img.save(internal_path)
            else:
                with open(internal_path, "wb") as f:
                    f.write(contents)

        update_query = """
            UPDATE user_external_service_links
            SET name = %s, link = %s, description = %s, image_path = %s
            WHERE id = %s AND user_id = %s
        """
        await query_aiomysql(
            conn,
            update_query,
            (name, link, description, new_external_path, link_id, user_id),
        )

        return {"message": f'External service link "{name}" updated successfully!'}

# Note: If a user is deleted, linked images remain on disk since the links are cascade-deleted.
# This isn't critical, and handling it would add unnecessary complexity considering the scope of the project.

@router.delete("/external_service_links/{link_id}")
async def delete_external_service_link(link_id: int, data: dict):
    async with aiomysql_conn_get() as conn:
        user_id = await validate_session_key_conn(conn, data.get("session_key"), guest_lock=True)

        # Fetch image path
        rows = await query_aiomysql(
            conn,
            "SELECT image_path FROM user_external_service_links WHERE user_id = %s AND id = %s LIMIT 1",
            (user_id, link_id),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Service link not found")

        image_path = rows[0]["image_path"]

        # Delete DB record
        delete_query = """
            DELETE FROM user_external_service_links
            WHERE user_id = %s AND id = %s
        """
        await query_aiomysql(conn, delete_query, (user_id, link_id))

        # Delete image file if exists
        if image_path and os.path.exists(image_path):
            os.remove(image_path)

        return {"message": "External service link deleted successfully!"}


@router.get("/external_service_links")
async def get_external_service_links(
    session_key: str = Query(...)
):
    async with aiomysql_conn_get() as conn:
        # Validate the session key and get the guest id if not session_key
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

        # Retrieve the links
        query = """
            SELECT id, name, link, description, image_path
            FROM user_external_service_links
            WHERE user_id = %s
            ORDER BY id DESC;
        """
        rows = await query_aiomysql(conn, query, (user_id,))

        return {"links": rows}