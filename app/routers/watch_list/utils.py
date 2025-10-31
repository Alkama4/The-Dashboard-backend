# External imports
import json
from typing import Tuple, List, Any
# Internal imports
from utils import query_aiomysql
from models.watch_list import TitleQueryParams


# ############## GET TITLES ##############

def _build_where_clause(
    user_id: int,
    params_obj: TitleQueryParams
) -> Tuple[str, List[Any]]:
    """
    Construct a WHERE clause (and its bound parameters) that can be reused by
    both the list-query builder and the count-query builder.
    """
    # Pull individual fields out of the Pydantic model for readability
    title_type = params_obj.title_type
    search_term = params_obj.search_term
    collection_id = params_obj.collection_id
    in_watchlist = params_obj.in_watchlist
    watch_status = params_obj.watch_status
    favourite = params_obj.favourite
    released = params_obj.released
    season_in_progress = params_obj.season_in_progress
    has_media_entry = params_obj.has_media_entry

    # Start with a no-op condition so we can safely join with AND
    conditions: List[str] = ["1=1"]
    bind_vals: List[Any] = [user_id]

    # ---- Watchlist filter --------------------------------------------------
    if in_watchlist is True:
        conditions.append("utd.user_id = %s")
        bind_vals.append(user_id)
    elif in_watchlist is False:
        conditions.append("(utd.user_id != %s OR utd.user_id IS NULL)")
        bind_vals.append(user_id)

    # ---- Type filter --------------------------------------------------------
    if title_type:
        conditions.append("t.type = %s")
        bind_vals.append(title_type.lower())

    # ---- Watch status sub-clauses ------------------------------------------
    if watch_status == "unwatched":
        conditions.append("COALESCE(utd.watch_count, 0) = 0")

    elif watch_status == "partially_watched":
        conditions.append("""
            t.type = 'tv'
            AND utd.watch_count = 0
            AND EXISTS (
                SELECT 1 FROM user_episode_details ued
                JOIN episodes e ON e.episode_id = ued.episode_id
                WHERE e.title_id = t.title_id
                    AND ued.user_id = utd.user_id
                    AND ued.watch_count > 0
            )
        """)

    elif watch_status == "fully_watched":
        conditions.append("utd.watch_count >= 1")


    # ---- Favourite ---------------------------------------------------------
    if favourite is True:
        conditions.append("utd.favourite = TRUE")
    elif favourite is False:
        conditions.append("utd.favourite = FALSE")

    # ---- Released ----------------------------------------------------------
    if released is True:
        conditions.append("t.release_date <= CURDATE()")
    elif released is False:
        conditions.append("t.release_date > CURDATE()")

    # ---- Season in progress -----------------------------------------------
    if season_in_progress is False:
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM seasons s
                JOIN episodes e ON e.season_id = s.season_id
                LEFT JOIN user_episode_details ued
                  ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                WHERE s.title_id = t.title_id
                GROUP BY s.season_id
                HAVING COUNT(DISTINCT COALESCE(ued.watch_count,0)) > 1
            )
        """)
    elif season_in_progress is True:
        conditions.append("""
            EXISTS (
                SELECT 1 FROM seasons s
                JOIN episodes e ON e.season_id = s.season_id
                LEFT JOIN user_episode_details ued
                  ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                WHERE s.title_id = t.title_id
                GROUP BY s.season_id
                HAVING COUNT(DISTINCT COALESCE(ued.watch_count,0)) > 1
            )
        """)

    # ---- Search term -------------------------------------------------------
    if search_term:
        # Look for the term in either name or original name
        conditions.append("(t.name LIKE %s OR t.name_original LIKE %s)")
        bind_vals.extend([f"%{search_term}%", f"%{search_term}%"])

    # ---- Collection filter -----------------------------------------------
    if collection_id is not None:
        conditions.append("ct.collection_id = %s")
        bind_vals.append(collection_id)

    # ---- Media entry filter -----------------------------------------------
    if has_media_entry is True:
        conditions.append(
            "EXISTS (SELECT 1 FROM title_media_details tmd WHERE tmd.title_id = t.title_id)"
        )
    elif has_media_entry is False:
        conditions.append(
            "NOT EXISTS (SELECT 1 FROM title_media_details tmd WHERE tmd.title_id = t.title_id)"
        )

    # Combine all parts into a single string
    where_sql = " AND ".join(conditions)
    return where_sql, bind_vals


def build_titles_query(
    user_id: int,
    params: TitleQueryParams
):
    """
    Build the full paginated SELECT for titles, including ordering.
    """
    # Base SELECT (unchanged from original implementation)
    base_query = """
        SELECT
            t.*,
            (SELECT COUNT(season_id) FROM seasons WHERE title_id = t.title_id) AS season_count,
            (SELECT COUNT(episode_id) FROM episodes WHERE title_id = t.title_id) AS episode_count,
            utd.favourite,
            utd.last_updated,
            utd.watch_count,
            CASE
                WHEN t.type = 'tv' THEN
                    EXISTS (
                        SELECT 1 FROM episodes e
                        LEFT JOIN user_episode_details ued ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                        WHERE e.title_id = t.title_id
                          AND e.air_date <= CURDATE()
                          AND e.air_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                          AND COALESCE(ued.watch_count, 0) <> 1
                        LIMIT 1
                    )
                ELSE FALSE
            END AS new_episodes,
            CASE WHEN utd.title_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_in_watchlist,
            GROUP_CONCAT(
                CASE WHEN uc.user_id = utd.user_id THEN uc.name END
                ORDER BY uc.name ASC SEPARATOR ', '
            ) AS collections,
            GROUP_CONCAT(DISTINCT g.genre_name ORDER BY g.genre_name SEPARATOR ', ') AS genres,
            (SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'image_id', ti.image_id,
                    'type', ti.type,
                    'format', ti.format,
                    'position', ti.position,
                    'is_primary', ti.is_primary,
                    'source_url', ti.source_url
                )
             )
             FROM title_images ti
             WHERE ti.title_id = t.title_id
            ) AS title_images
        FROM titles t
        LEFT JOIN user_title_details utd ON utd.title_id = t.title_id AND utd.user_id = %s
        LEFT JOIN collection_title ct ON ct.title_id = t.title_id
        LEFT JOIN user_collection uc ON uc.collection_id = ct.collection_id
        LEFT JOIN title_genres tg ON tg.title_id = t.title_id
        LEFT JOIN genres g ON g.genre_id = tg.genre_id
    """

    where_sql, bind_vals = _build_where_clause(user_id, params)

    # Assemble the full query
    query = base_query + " WHERE " + where_sql
    query += " GROUP BY t.title_id"

    # Ordering (same as original)
    sort_map = {
        "rating": "t.tmdb_vote_average",
        "popularity": "t.tmdb_vote_count",
        "release_date": "t.release_date",
        "title_name": "t.name",
        "duration": """
            CASE
                WHEN t.type = 'movie' THEN t.movie_runtime
                WHEN t.type = 'tv' THEN (
                    SELECT COALESCE(SUM(e.runtime), 0)
                    FROM episodes e
                    WHERE e.title_id = t.title_id
                )
                ELSE NULL
            END""",
        "data_updated": "t.last_updated",
        "modified": """
            GREATEST(
                utd.last_updated,
                (
                    SELECT MAX(ued.last_updated)
                    FROM episodes e
                    LEFT JOIN user_episode_details ued
                        ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                    WHERE e.title_id = t.title_id
                )
            )
        """
    }
    order_column = sort_map.get(params.sort_by, """
        GREATEST(
            utd.last_updated,
            (
                SELECT MAX(ued.last_updated)
                FROM episodes e
                LEFT JOIN user_episode_details ued
                    ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                WHERE e.title_id = t.title_id
            )
        )"""
    )
    direction = params.direction or "DESC"
    query += f" ORDER BY {order_column} {direction}"

    # Pagination
    if params.title_limit:
        query += " LIMIT %s OFFSET %s"
        bind_vals.extend([params.title_limit,
                          (params.page - 1) * params.title_limit])

    return query, bind_vals


def build_titles_count_query(
    user_id: int,
    params: TitleQueryParams
):
    """
    Build a simple COUNT(*) query that re-uses the same WHERE clause.
    Pagination and ordering are omitted intentionally.
    """
    where_sql, bind_vals = _build_where_clause(user_id, params)

    # Optional join to collection_title if a collection filter is present
    collection_join = (
        "LEFT JOIN collection_title ct ON ct.title_id = t.title_id"
        if params.collection_id is not None
        else ""
    )

    count_query = f"""
        SELECT COUNT(DISTINCT t.title_id) AS total
        FROM titles t
        LEFT JOIN user_title_details utd ON utd.title_id = t.title_id AND utd.user_id = %s
        {collection_join}
        WHERE {where_sql}
    """

    return count_query, bind_vals


def map_title_row(row):
    row["collections"] = row["collections"].split(", ") if row["collections"] else []
    row["genres"] = row["genres"].split(", ") if row["genres"] else []

    title_images = json.loads(row["title_images"]) if row["title_images"] else []
    title_images_dict = {}
    for img in title_images:
        img_obj = img.copy()
        img_obj["path"] = f"/image/title/{row['title_id']}/{img['image_id']}.{img['format']}"
        title_images_dict.setdefault(img["type"], []).append(img_obj)
    row["title_images"] = title_images_dict
    return row



# ############## QUICK CONVERTS ##############

# Converters between title and tmdb ids
async def title_to_tmdb_id(conn, title_id):
        query = """
            SELECT tmdb_id
            FROM titles
            WHERE title_id = %s
        """
        result = await query_aiomysql(conn, query, (title_id,), use_dictionary=False)
        return result[0][0]


async def tmdb_to_title_id(conn, tmdb_id):
        query = """
            SELECT title_id
            FROM titles
            WHERE tmdb_id = %s
        """
        result = await query_aiomysql(conn, query, (tmdb_id,), use_dictionary=False)
        return result[0][0]


async def convert_season_or_episode_id_to_title_id(conn, season_id=None, episode_id=None):
    if season_id:
        # Get the title_id from the season_id
        get_title_id_query = """
            SELECT title_id
            FROM seasons
            WHERE season_id = %s
        """
        result = await query_aiomysql(conn, get_title_id_query, (season_id,), use_dictionary=False)
        title_id = result[0][0] if result else None
    elif episode_id:
        # Get the title_id from the episode_id
        get_title_id_query = """
            SELECT title_id
            FROM episodes
            WHERE episode_id = %s
        """
        result = await query_aiomysql(conn, get_title_id_query, (episode_id,), use_dictionary=False)
        title_id = result[0][0] if result else None
    else:
        title_id = None

    return title_id


# ############## FORMAT ##############

def format_FI_age_rating(rating):
    rating = rating.upper()
    if rating == 'S':
        return rating
    if 'K' not in rating:
        rating = 'K' + rating
    if '-' not in rating:
        rating = rating[:1] + '-' + rating[1:]
    return rating
