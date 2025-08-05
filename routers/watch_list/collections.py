# External imports
from fastapi import HTTPException, APIRouter, Query

# Internal imports
from utils import (
    validate_session_key_conn,
    aiomysql_connect,
    query_aiomysql,
)
from .utils import (
    build_titles_query,
)

router = APIRouter()


async def check_collection_ownership(conn, collection_id: int, user_id: int):
    query = """
        SELECT 1 FROM user_collection
        WHERE collection_id = %s AND user_id = %s
    """
    result = await query_aiomysql(conn, query, (collection_id, user_id))
    if not result:
        raise HTTPException(status_code=403, detail="You do not own this collection.")


@router.post("")
async def create_collection(data: dict):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    name = data.get("name")
    description = data.get("description")

    if (not name):
        raise HTTPException(status_code=400, detail=f"Missing required parameter: name")
    
    query = """
        INSERT INTO user_collection (user_id, name, description)
        VALUES (%s, %s, %s)
    """
    collection_id = await query_aiomysql(conn, query, (user_id, name, description), return_lastrowid=True)
    conn.close()

    return {
        "message": "Collection created successfully!",
        "collection": {
            'collection_id': collection_id,
            'name': name, 
            'description': description
        }
    }


@router.put("/{collection_id}")
async def edit_collection(collection_id: int, data: dict):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    name = data.get("name")
    description = data.get("description")

    fields = []
    values = []

    if name is not None:
        fields.append("name = %s")
        values.append(name)
    if description is not None:
        fields.append("description = %s")
        values.append(description)

    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    query = f"""
        UPDATE user_collection
        SET {', '.join(fields)}
        WHERE collection_id = %s AND user_id = %s
    """
    values.extend([collection_id, user_id])
    await query_aiomysql(conn, query, tuple(values))

    return {
        "message": "Collection updated successfully!"
    }


@router.delete("/{collection_id}")
async def delete_collection(collection_id: int, data: dict):
    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    await check_collection_ownership(conn, collection_id, user_id)

    query = """
        DELETE FROM user_collection 
        WHERE user_id = %s AND collection_id = %s
    """
    await query_aiomysql(conn, query, (user_id, collection_id))
    conn.close()

    return {
        "message": "Collection deleted successfully!"
    }


@router.get("")
async def list_collections(session_key: str = Query(...)):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, session_key)

    query = """
        SELECT
            collection_id,
            name,
            description,
            parent_collection_id
        FROM user_collection
        WHERE user_id = %s
        ORDER BY name
    """
    collections = await query_aiomysql(conn, query, (user_id,))

    collection_map = {c['collection_id']: {**c, 'titles': [], 'children': []} for c in collections}

    for collection in collection_map.values():
        query, query_params = build_titles_query(
            user_id,
            collection_id=collection['collection_id'],
            sort_by='release_date',
            direction='ASC',
            offset=0,
        )
        titles = await query_aiomysql(conn, query, tuple(query_params))
        for row in titles:
            row["collections"] = row["collections"].split(", ") if row["collections"] else []
        collection['titles'] = titles

    conn.close()

    roots = []
    for collection in collection_map.values():
        parent_id = collection['parent_collection_id']
        if parent_id:
            collection_map[parent_id]['children'].append(collection)
        else:
            roots.append(collection)

    return roots


@router.put("/{collection_id}/title/{title_id}")
async def add_title_to_collection(collection_id: int, title_id: int, data: dict):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    await check_collection_ownership(conn, collection_id, user_id)

    query = """
        INSERT INTO collection_title (collection_id, title_id)
        VALUES (%s, %s)
    """
    await query_aiomysql(conn, query, (collection_id, title_id))
    conn.close()

    return {
        "message": "Title added successfully to the collection!"
    }


@router.delete("/{collection_id}/title/{title_id}")
async def remove_title_from_collection(collection_id: int, title_id: int, data: dict):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    await check_collection_ownership(conn, collection_id, user_id)

    query = """
        DELETE FROM collection_title 
        WHERE collection_id = %s AND title_id = %s
    """
    await query_aiomysql(conn, query, (collection_id, title_id))
    conn.close()

    return {
        "message": "Title removed successfully from the collection!"
    }
