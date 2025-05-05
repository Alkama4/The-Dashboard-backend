# External imports
from datetime import timedelta
from fastapi import HTTPException, APIRouter, Query

# Internal imports
from utils import (
    query_tmdb,
    add_to_cache,
    get_from_cache,
    validate_session_key_conn,
    aiomysql_conn_get,
    aiomysql_connect,
    query_aiomysql,
)
from .utils import (
    keep_title_watch_count_up_to_date,
)

# Child routers
from .titles import router as title_router
from .collections import router as collection_router

# Create the router object for this module
router = APIRouter()
router.include_router(title_router, prefix="/titles", tags=["titles"])
router.include_router(collection_router, prefix="/collections", tags=["collections"])


@router.put("/seasons/{season_id}/watch_count")
async def update_season_watch_count(season_id: int, data: dict):
    conn = await aiomysql_connect()
    try:
        user_id = await validate_session_key_conn(conn, data.get("session_key"))

        watch_count = data.get("watch_count")

        if not isinstance(watch_count, int) or watch_count < 0:
            raise HTTPException(status_code=400, detail="watch_count must be a non-negative integer")

        query = """
            INSERT INTO user_episode_details (user_id, episode_id, watch_count)
            SELECT %s, episode_id, %s
            FROM episodes
            WHERE season_id = %s
            ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count)
        """
        await query_aiomysql(conn, query, (user_id, watch_count, season_id))
        await keep_title_watch_count_up_to_date(conn, user_id, season_id=season_id)

        conn.close()

        return {"message": "Season watch count updated!"}
    
    finally:
        conn.close()


@router.put("/episodes/{episode_id}/watch_count")
async def update_episode_watch_count(episode_id: int, data: dict):
    try:
        conn = await aiomysql_connect()
        user_id = await validate_session_key_conn(conn, data.get("session_key"))
        watch_count = data.get("watch_count")

        if not isinstance(watch_count, int) or watch_count < 0:
            raise HTTPException(status_code=400, detail="watch_count must be a non-negative integer")

        query = """
            INSERT INTO user_episode_details (user_id, episode_id, watch_count)
            VALUES (%s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE watch_count = new.watch_count
        """
        await query_aiomysql(conn, query, (user_id, episode_id, watch_count))
        await keep_title_watch_count_up_to_date(conn, user_id, episode_id=episode_id)

        conn.close()

        return {"message": "Episode watch count updated!"}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# Acts as a middle man between TMDB search and vue. 
# Adds proper genres and the fact wether the user has added the title or not.
@router.get("/search")
async def watch_list_search(
    session_key: str = Query(...),
    title_category: str = Query(..., regex="^(movie|tv)$"),
    title_name: str = Query(None),
):
    async with aiomysql_conn_get() as conn:
        # Validate the session key and retrieve the user ID
        user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)


        # Fetch search results from cache or TMDB API
        if not title_name:
            raise HTTPException(status_code=400, detail="Title name is required.")

        title_lower = title_name.lower()

        # Try to get from Redis cache first
        search_results = await get_from_cache(title_lower)
        if search_results is None:
            found_from_cache = False
            search_results = await query_tmdb(
                f"/search/{title_category}",
                {"query": title_name, "include_adult": False}
            )
            # Store results in Redis cache
            await add_to_cache(title_lower, search_results, timedelta(weeks=1))
        else:
            found_from_cache = True
            print(f"Found \"{title_lower}\" from redis. Using it instead of querying TMDB.")

        # Retrieve genre mappings from the database
        genre_query = "SELECT tmdb_genre_id, genre_name FROM genres"
        genre_data = await query_aiomysql(conn, genre_query, use_dictionary=False)
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
            title_id_data = await query_aiomysql(conn, title_id_query, (*tmdb_ids,), use_dictionary=False)
            title_id_dict = {row[0]: row[1] for row in title_id_data}

            # Query to get user's watchlist details
            watchlist_query = f"""
                SELECT t.tmdb_id, t.title_id
                FROM user_title_details utd
                JOIN titles t ON utd.title_id = t.title_id
                WHERE utd.user_id = %s AND t.tmdb_id IN ({placeholders})
            """
            watchlist_data = await query_aiomysql(conn, watchlist_query, (user_id, *tmdb_ids), use_dictionary=False)
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

        return {
            'result': search_results,
            'used_cache': found_from_cache
        }


# Used to manually update the genres if they change etc. In the past was ran always on start, but since it ran on all 4 workers the feature was removed. Basically only used if I were to wipe the whole db.

# DO NOT call if not necessary. Have not been recently tested and will cause unnescary problems
@router.put("/genres")
async def update_genres():
    async with aiomysql_conn_get() as conn:

        # Fetch movie genres
        movie_genres = await query_tmdb("/genre/movie/list", {})
        if movie_genres:
            for genre in movie_genres.get("genres", []):

                genre_id = genre.get("id")
                genre_name = genre.get("name")
                
                # Check if genre exists in the database
                query = "SELECT * FROM genres WHERE tmdb_genre_id = %s"
                existing_genre = await query_aiomysql(conn, query, (genre_id,), use_dictionary=False)

                if not existing_genre:
                    # Insert new genre
                    query = "INSERT INTO genres (tmdb_genre_id, genre_name) VALUES (%s, %s)"
                    await query_aiomysql(conn, query, (genre_id, genre_name))

                else:
                    # Update genre if name changes
                    query = "UPDATE genres SET genre_name = %s WHERE tmdb_genre_id = %s"
                    await query_aiomysql(conn, query, (genre_name, genre_id))

            print("Movie genres stored!")

        # Fetch TV genres
        tv_genres = query_tmdb("/genre/tv/list", {})
        if tv_genres:
            for genre in tv_genres.get("genres", []):
                genre_id = genre.get("id")
                genre_name = genre.get("name")
                # Check if genre exists in the database
                query = "SELECT * FROM genres WHERE tmdb_genre_id = %s"
                existing_genre = await query_aiomysql(conn, query, (genre_id,), use_dictionary=False)
                
                if not existing_genre:
                    # Insert new genre
                    query = "INSERT INTO genres (tmdb_genre_id, genre_name) VALUES (%s, %s)"
                    await query_aiomysql(conn, query, (genre_id, genre_name))

                else:
                    # Update genre if name changes
                    query = "UPDATE genres SET genre_name = %s WHERE tmdb_genre_id = %s"
                    await query_aiomysql(conn, query, (genre_name, genre_id))

            print("TV genres stored!")

        return {"Result": "Genres updated!",}



# Sort by options for future "/list_titles":
    # Vote average (default)
    # Last watched
    # Alpabetical
    # Popularity (amount of votes)
    # Duration / Episode Count

# When updating watch count query the values for the title inside the updating endpoint and return them. 

# To add:
# production_companies (new table) and image function for their images
# production_companies (new table) and image function for their images
# production_companies (new table) and image function for their images
# production_companies (new table) and image function for their images
