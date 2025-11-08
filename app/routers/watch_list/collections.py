# External imports
from fastapi import HTTPException, APIRouter, Query

# Internal imports
from app.utils import (
    validate_session_key_conn,
    aiomysql_connect,
    query_aiomysql,
)
from .utils import (
    build_titles_query,
    build_titles_count_query,
    map_title_row
)
from app.models.watch_list import TitleQueryParams

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
async def list_collections(session_key: str = Query(None)):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, session_key)

    query = """
        SELECT
            c.collection_id,
            c.name,
            c.description,
            c.parent_collection_id,
            COUNT(DISTINCT t.title_id) AS total_count,
            MIN(CASE WHEN t.type = 'movie' THEN t.release_date ELSE e.air_date END) AS first_date,
            MAX(CASE WHEN t.type = 'movie' THEN t.release_date ELSE e.air_date END) AS last_date,
            SUM(CASE WHEN t.type = 'tv' THEN COALESCE(e.runtime, 0) ELSE t.movie_runtime END) AS total_length
        FROM user_collection c
        LEFT JOIN collection_title ct ON c.collection_id = ct.collection_id
        LEFT JOIN titles t ON ct.title_id = t.title_id
        LEFT JOIN episodes e ON t.type = 'tv' AND e.title_id = t.title_id
        WHERE c.user_id = %s
        GROUP BY c.collection_id
        ORDER BY c.name
    """
    collections = await query_aiomysql(conn, query, (user_id,))

    collection_map = {c['collection_id']: {**c, 'titles': [], 'children': []} for c in collections}

    for collection in collection_map.values():
        query, query_params = build_titles_query(user_id, TitleQueryParams(
            collection_id=collection['collection_id'],
            sort_by='release_date',
            direction='ASC',
            title_limit=4
        ))
        
        raw_titles = await query_aiomysql(conn, query, tuple(query_params))
        collection['preview_titles'] = [map_title_row(row) for row in (raw_titles or [])]

    conn.close()

    roots = []
    for collection in collection_map.values():
        parent_id = collection['parent_collection_id']
        if parent_id:
            collection_map[parent_id]['children'].append(collection)
        else:
            roots.append(collection)

    return roots


@router.get("/{collection_id}")
async def get_collection(
    collection_id: int,
    session_key: str = Query(None)
):
    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, session_key)

    query = """
        SELECT
            c.collection_id,
            c.name,
            c.description,
            c.parent_collection_id,
            COUNT(DISTINCT t.title_id) AS total_count,
            MIN(CASE WHEN t.type = 'movie' THEN t.release_date ELSE e.air_date END) AS first_date,
            MAX(CASE WHEN t.type = 'movie' THEN t.release_date ELSE e.air_date END) AS last_date,
            SUM(CASE WHEN t.type = 'tv' THEN COALESCE(e.runtime, 0) ELSE t.movie_runtime END) AS total_length
        FROM user_collection c
        LEFT JOIN collection_title ct ON c.collection_id = ct.collection_id
        LEFT JOIN titles t ON ct.title_id = t.title_id
        LEFT JOIN episodes e ON t.type = 'tv' AND e.title_id = t.title_id
        WHERE c.user_id = %s
            AND (c.collection_id = %s OR c.parent_collection_id = %s)
        GROUP BY c.collection_id
        ORDER BY c.name
    """
    result = await query_aiomysql(conn, query, (user_id, collection_id, collection_id))
    if not result:
        conn.close()
        return None

    # Separate parent from children
    parent = None
    children = []
    for row in result:
        if row['collection_id'] == collection_id:
            parent = {**row, 'titles': [], 'children': []}
        else:
            children.append({**row, 'titles': [], 'children': []})

    if not parent:
        conn.close()
        return None

    # Attach children to parent
    parent['children'] = children

    # Fetch titles for parent
    query, query_params = build_titles_query(
        user_id,
        params=TitleQueryParams(
            collection_id=parent['collection_id'],
            sort_by='release_date',
            direction='ASC',
            offset=0,
        )
    )
    titles = await query_aiomysql(conn, query, tuple(query_params))
    parent['titles'] = [map_title_row(row) for row in (titles or [])]

    # Fetch titles for each child
    for child in children:
        query, query_params = build_titles_query(
            user_id,
            params=TitleQueryParams(
                collection_id=child['collection_id'],
                sort_by='release_date',
                direction='ASC',
                title_limit=4
            )
        )
        
        raw_titles = await query_aiomysql(conn, query, tuple(query_params))
        child['preview_titles'] = [map_title_row(row) for row in (raw_titles or [])]

    conn.close()
    return parent


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
