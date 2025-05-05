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
    title_type: Optional[str] = None, 
    watched: Optional[bool] = None, 
    favourite: Optional[bool] = None, 
    released: Optional[bool] = None, 
    started: Optional[bool] = None, 
    all_titles: Optional[str] = None, 
    search_term: Optional[str] = None, 
    collection_id: Optional[int] = None, 
    sort_by: Optional[str] = None, 
    direction: Optional[str] = None, 
    offset: int = 0, 
    title_limit: Optional[int] = None
):
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
            (SELECT COUNT(season_id) FROM seasons WHERE title_id = t.title_id) AS season_count,
            (SELECT COUNT(episode_id) FROM episodes WHERE title_id = t.title_id) AS episode_count,
            utd.favourite,
            utd.last_updated,
            EXISTS (
                SELECT 1
                FROM episodes e
                LEFT JOIN user_episode_details ued
                    ON ued.episode_id = e.episode_id
                    AND ued.user_id = utd.user_id
                WHERE e.title_id = t.title_id
                    AND e.air_date <= CURDATE()
                    AND e.air_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                    AND COALESCE(ued.watch_count, 0) != 1
                LIMIT 1
            ) AS new_episodes,
            CASE
                WHEN utd.title_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS is_in_watchlist,
            GROUP_CONCAT(DISTINCT g.genre_name ORDER BY g.genre_name ASC SEPARATOR ', ') AS genres
        FROM
            titles t
        LEFT JOIN
            user_title_details utd ON utd.title_id = t.title_id AND utd.user_id = %s
        LEFT JOIN
            title_genres tg ON tg.title_id = t.title_id
        LEFT JOIN
            genres g ON tg.genre_id = g.genre_id
        LEFT JOIN
            collection_title ct ON ct.title_id = t.title_id
        WHERE
            1=1
    """
    query_params = [user_id]

    if all_titles == "all_titles":
        pass
    elif all_titles == "not_added":
        query += " AND (utd.user_id != %s OR utd.user_id IS NULL)"
        query_params.append(user_id)
    else:
        query += " AND utd.user_id = %s"
        query_params.append(user_id)

    if title_type:
        query += " AND t.type = %s"
        query_params.append(title_type.lower())

    if watched is True:
        query += " AND utd.watch_count >= 1"
    elif watched is False:
        query += " AND (utd.watch_count <= 0 OR utd.watch_count IS NULL)"

    if favourite is True:
        query += " AND utd.favourite = TRUE"
    elif favourite is False:
        query += " AND utd.favourite = FALSE"

    if released is True:
        query += " AND t.release_date <= CURDATE()"
    elif released is False:
        query += " AND t.release_date > CURDATE()"

    if started is True:
        query += """
            AND EXISTS (
                SELECT 1
                FROM user_episode_details ued
                JOIN episodes e ON ued.episode_id = e.episode_id
                WHERE ued.user_id = utd.user_id
                AND e.title_id = t.title_id
                AND ued.watch_count > 0
                LIMIT 1
            )
        """
    elif started is False:
        query += """
            AND NOT EXISTS (
                SELECT 1
                FROM user_episode_details ued
                JOIN episodes e ON ued.episode_id = e.episode_id
                WHERE ued.user_id = utd.user_id
                AND e.title_id = t.title_id
                AND ued.watch_count > 0
                LIMIT 1
            )
        """

    if search_term:
        query += " AND t.name LIKE %s"
        query_params.append(f"%{search_term}%")

    if collection_id is not None:
        query += " AND ct.collection_id = %s"
        query_params.append(collection_id)

    query += " GROUP BY t.title_id"

    direction = direction.upper() if direction else "DESC"

    if sort_by == "release_date":
        query += f" ORDER BY t.release_date {direction}"
    elif sort_by == "latest_updated":
        query += f" ORDER BY utd.last_updated {direction}"
    else:
        query += f" ORDER BY t.tmdb_vote_average {direction}"

    if title_limit:
        query += " LIMIT %s OFFSET %s"
        query_params.extend([title_limit + 1, offset * title_limit])

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



# ############## CHECK FROM FILES ##############

def get_backdrop_count(title_id: int):
    # Path base
    base_path = "/fastapi-media/title"
    
    # Count of backdrops that exist
    count = 0

    # Loop through the backdrops from 1 to 5
    for i in range(1, 6):
        image_path = os.path.join(base_path, str(title_id), f"backdrop{i}.jpg")
        
        # Check if the image exists
        if os.path.exists(image_path):
            count += 1

    return count


def get_logo_type(title_id: int):
    # Base path
    base_path = "/fastapi-media/title"
    
    # Possible logo types/extensions to check
    logo_types = ["png", "svg", "jpg", "jpeg", "webp"]

    # Check for each type
    for logo_type in logo_types:
        logo_path = os.path.join(base_path, str(title_id), f"logo.{logo_type}")
        if os.path.exists(logo_path):
            return logo_type  # Return the first found type

    return None  # Return None if no logo exists



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
