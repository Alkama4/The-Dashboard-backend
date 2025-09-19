# External imports
from typing import Optional
import os

# Internal imports
from utils import (
    query_aiomysql,
)


# ############## GET TITLES ##############

def build_titles_query(
    user_id: int, 
    
    # Filters 
    # String or int values (if missing don't filter)
    title_type: Optional[str] = None, 
    search_term: Optional[str] = None, 
    collection_id: Optional[int] = None, 
    # value1 (true) and value2 (false) (if missing don't filter)
    in_watchlist: Optional[bool] = None, 
    watched: Optional[bool] = None,
    favourite: Optional[bool] = None,
    released: Optional[bool] = None, 
    title_in_progress: Optional[bool] = None, 
    season_in_progress: Optional[bool] = None, 
    
    # Sorting
    sort_by: Optional[str] = None, 
        # last_updated (default)
        # rating
        # popularity
        # release_date
        # title_name
        # duration
        # data_updated
    direction: Optional[str] = None, 
        # DESC/desc (default)
        # ASC/asc

    # Offset and limit
    offset: int = 0, 
    title_limit: Optional[int] = None
):
    # - - - - - - Baseline - - - - - -
    # Base query
    query = """
        SELECT
            t.title_id,
            t.tmdb_id,
            t.name,
            t.name_original,
            t.tmdb_vote_average,
            t.tmdb_vote_count,
            t.movie_runtime,
            COALESCE(utd.watch_count, 0) AS watch_count,
            t.type,
            t.release_date,
            t.backup_poster_url,
            t.overview,
            t.age_rating,
            (SELECT COUNT(season_id) FROM seasons WHERE title_id = t.title_id) AS season_count,
            (SELECT COUNT(episode_id) FROM episodes WHERE title_id = t.title_id) AS episode_count,
            utd.favourite,
            utd.last_updated,
            CASE
                WHEN t.type = 'tv' THEN
                    EXISTS (
                        SELECT 1
                        FROM episodes e
                        LEFT JOIN user_episode_details ued
                            ON ued.episode_id = e.episode_id
                            AND ued.user_id = utd.user_id
                        WHERE e.title_id = t.title_id
                        AND e.air_date <= CURDATE()
                        AND e.air_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                        AND COALESCE(ued.watch_count, 0) <> 1
                        LIMIT 1
                    )
                ELSE FALSE
            END AS new_episodes,
            CASE
                WHEN utd.title_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_in_watchlist,
            GROUP_CONCAT(
                CASE
                    WHEN uc.user_id = utd.user_id THEN uc.name
                    ELSE NULL
                END
                ORDER BY uc.name ASC SEPARATOR ', '
            ) AS collections,
            GROUP_CONCAT(
                DISTINCT g.genre_name
                ORDER BY g.genre_name ASC SEPARATOR ', '
            ) AS genres
        FROM
            titles t
        LEFT JOIN
            user_title_details utd ON utd.title_id = t.title_id AND utd.user_id = %s
        LEFT JOIN
            collection_title ct ON ct.title_id = t.title_id
        LEFT JOIN
            user_collection uc ON uc.collection_id = ct.collection_id
        LEFT JOIN
            title_genres tg ON tg.title_id = t.title_id
        LEFT JOIN
            genres g ON g.genre_id = tg.genre_id
        WHERE
            1=1
    """

    # Base params
    query_params = [user_id]

    # - - - - - - Filters - - - - - -
    # In watch list/not in watchlist/do not care
    if in_watchlist is True:
        query += " AND utd.user_id = %s"
        query_params.append(user_id)
    elif in_watchlist is False:
        query += " AND (utd.user_id != %s OR utd.user_id IS NULL)"
        query_params.append(user_id)
    # if None, do nothing (all titles)

    # Set title type (tv, movie)
    if title_type:
        query += " AND t.type = %s"
        query_params.append(title_type.lower())

    # Boolean is watched or not (could be a number)
    if watched is True:
        query += " AND utd.watch_count >= 1"
    elif watched is False:
        query += " AND (utd.watch_count <= 0 OR utd.watch_count IS NULL)"

    # Check if favourite
    if favourite is True:
        query += " AND utd.favourite = TRUE"
    elif favourite is False:
        query += " AND utd.favourite = FALSE"

    # Check if released
    if released is True:
        query += " AND t.release_date <= CURDATE()"
    elif released is False:
        query += " AND t.release_date > CURDATE()"

    # Check if all episodes have same watch_count
    if title_in_progress is False:
        query += """
            AND EXISTS (
                SELECT 1
                FROM episodes e
                LEFT JOIN user_episode_details ued
                    ON ued.episode_id = e.episode_id
                    AND ued.user_id = utd.user_id
                WHERE e.title_id = t.title_id
                GROUP BY e.title_id
                HAVING COUNT(DISTINCT COALESCE(ued.watch_count, 0)) = 1
            )
        """
    elif title_in_progress is True:
        query += """
            AND NOT EXISTS (
                SELECT 1
                FROM episodes e
                LEFT JOIN user_episode_details ued
                    ON ued.episode_id = e.episode_id
                    AND ued.user_id = utd.user_id
                WHERE e.title_id = t.title_id
                GROUP BY e.title_id
                HAVING COUNT(DISTINCT COALESCE(ued.watch_count, 0)) = 1
            )
        """

    # Check if all episodes within a season have same watch_count
    if season_in_progress is False:
        query += """
            AND NOT EXISTS (
                SELECT 1
                FROM seasons s
                JOIN episodes e ON e.season_id = s.season_id
                LEFT JOIN user_episode_details ued
                    ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                WHERE s.title_id = t.title_id
                GROUP BY s.season_id
                HAVING COUNT(DISTINCT COALESCE(ued.watch_count, 0)) > 1
            )
        """
    elif season_in_progress is True:
        query += """
            AND EXISTS (
                SELECT 1
                FROM seasons s
                JOIN episodes e ON e.season_id = s.season_id
                LEFT JOIN user_episode_details ued
                    ON ued.episode_id = e.episode_id AND ued.user_id = utd.user_id
                WHERE s.title_id = t.title_id
                GROUP BY s.season_id
                HAVING COUNT(DISTINCT COALESCE(ued.watch_count, 0)) > 1
            )
        """

    # Filter by keyword (search)
    if search_term:
        query += " AND t.name LIKE %s"
        query_params.append(f"%{search_term}%")

    # Add single collection filter
    if collection_id is not None:
        query += " AND ct.collection_id = %s"
        query_params.append(collection_id)

    # Add group by
    query += " GROUP BY t.title_id"

    # - - - - - - Sorting - - - - - -
    # Set direction (default descending)
    direction = direction.upper() if direction else "DESC"

    # Use the sorting parameter
    if sort_by == "rating":
        query += f" ORDER BY t.tmdb_vote_average {direction}"
    elif sort_by == "popularity":
        query += f"  ORDER BY t.tmdb_vote_count {direction}"
    elif sort_by == "release_date":
        query += f" ORDER BY t.release_date {direction}"
    elif sort_by == "title_name":
        query += f" ORDER BY t.name {direction}"
    elif sort_by == "duration":
        query += f"""
            ORDER BY
                CASE
                    WHEN t.type = 'movie' THEN t.movie_runtime
                    WHEN t.type = 'tv' THEN (
                        SELECT COALESCE(SUM(e.runtime), 0)
                        FROM episodes e
                        WHERE e.title_id = t.title_id
                    )
                    ELSE NULL
                END {direction}
        """
    elif sort_by == "data_updated":
        query += f" ORDER BY t.last_updated {direction}"
    else: # sort_by == "modified"
        query += f" ORDER BY utd.last_updated {direction}"

    # - - - - - - Limit and offset - - - - - -
    if title_limit:
        query += " LIMIT %s OFFSET %s"
        query_params.extend([title_limit, offset * title_limit])

    return query, query_params



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
