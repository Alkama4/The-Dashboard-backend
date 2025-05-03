# External imports
from datetime import timedelta
from fastapi import HTTPException, APIRouter, Query
from typing import Optional
from pathlib import Path
import asyncio
import os
import httpx
import json

# Internal imports
from utils import query_mysql, query_tmdb, query_omdb, download_image, validate_session_key, add_to_cache, fetch_user_settings, add_to_cache, get_from_cache, aiomysql_connect, aiomysql_conn_execute, validate_session_key_conn, get_aiomysql_conn
from custom_values import custom_combined_links

# Create the router object for this module
router = APIRouter()


# Converters between title and tmdb ids
async def title_to_tmdb_id(conn, title_id):
        query = """
            SELECT tmdb_id
            FROM titles
            WHERE title_id = %s
        """
        result = await aiomysql_conn_execute(conn, query, (title_id,), use_dictionary=False)
        return result[0][0]

async def tmdb_to_title_id(conn, tmdb_id):
        query = """
            SELECT title_id
            FROM titles
            WHERE tmdb_id = %s
        """
        result = await aiomysql_conn_execute(conn, query, (tmdb_id,), use_dictionary=False)
        return result[0][0]


# Used to get the images for a title and only the title. Seasons and episodes have a seperate one
async def store_title_images(movie_images, title_id: str, replace_images = False):
    try:
        base_path = f'/fastapi-media/title/{title_id}'
        Path(base_path).mkdir(parents=True, exist_ok=True)

        tasks = []

        # Get the first logo
        if 'logos' in movie_images:
            logo = movie_images['logos'][:1]  # Get only the first logo
            for idx, image in enumerate(logo):
                image_url = f"https://image.tmdb.org/t/p/original{image['file_path']}"
                file_extension = image['file_path'].split('.')[-1]
                image_filename = f"logo.{file_extension}"
                image_save_path = os.path.join(base_path, image_filename)
                tasks.append(download_image(image_url, image_save_path, replace_images))

        # Get the first poster
        if 'posters' in movie_images:
            # Filter posters to only include those with "iso_639_1" as "en"
            english_posters = [image for image in movie_images['posters'] if image.get('iso_639_1') == 'en']
            
            if english_posters:  # Check if there are any English posters
                first_english_poster = english_posters[0]  # Get the first English poster
                image_url = f"https://image.tmdb.org/t/p/original{first_english_poster['file_path']}"
                file_extension = first_english_poster['file_path'].split('.')[-1]
                image_filename = f"poster.{file_extension}"
                image_save_path = os.path.join(base_path, image_filename)
                tasks.append(download_image(image_url, image_save_path, replace_images))

        # Get the first 5 backdrops
        if 'backdrops' in movie_images:
            backdrops = movie_images['backdrops'][:5]  # Get the first 5 backdrops
            for idx, image in enumerate(backdrops):
                image_url = f"https://image.tmdb.org/t/p/original{image['file_path']}"
                file_extension = image['file_path'].split('.')[-1]
                image_filename = f"backdrop{idx + 1}.{file_extension}"
                image_save_path = os.path.join(base_path, image_filename)
                tasks.append(download_image(image_url, image_save_path, replace_images))

        # Run all the download tasks concurrently
        await asyncio.gather(*tasks)

        return {"success": True}

    except Exception as e:
        print(f"store_title_images error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def store_season_images(tv_seasons, title_id: str, replace_images = False):
    try:
        tasks = []

        for season in tv_seasons:
            season_number = season.get("season_number")
            poster_path = season.get("poster_path")

            if not poster_path or season_number == 0:  # Skip if no poster exists or if specials
                continue

            # Define base path for season
            season_path = f'/fastapi-media/title/{title_id}/season{season_number}'
            Path(season_path).mkdir(parents=True, exist_ok=True)

            # Construct image URL & save path
            image_url = f"https://image.tmdb.org/t/p/original{poster_path}"
            file_extension = poster_path.split('.')[-1]
            image_filename = f"poster.{file_extension}"
            image_save_path = os.path.join(season_path, image_filename)

            # Add download task
            tasks.append(download_image(image_url, image_save_path, replace_images))

        # Run all the download tasks concurrently
        await asyncio.gather(*tasks)

        return {"success": True}

    except Exception as e:
        print(f"store_season_images error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def store_episode_images(tv_episodes, title_id: str, replace_images = False):
    try:
        tasks = []

        for episode in tv_episodes:
            season_number = episode.get("season_number")
            episode_number = episode.get("episode_number")
            still_path = episode.get("still_path")

            if not still_path:  # Skip if no still image exists
                continue

            # Define base path for the episode image
            episode_path = f'/fastapi-media/title/{title_id}/season{season_number}'
            Path(episode_path).mkdir(parents=True, exist_ok=True)

            # Construct image URL & save path
            image_url = f"https://image.tmdb.org/t/p/original{still_path}"
            file_extension = still_path.split('.')[-1]
            image_filename = f"episode{episode_number}.{file_extension}"
            image_save_path = os.path.join(episode_path, image_filename)

            # Add download task
            tasks.append(download_image(image_url, image_save_path, replace_images))

        # Run all the download tasks concurrently
        await asyncio.gather(*tasks)

        return {"success": True}

    except Exception as e:
        print(f"store_episode_images error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# Used for the tvs and movies to add the genres to a title avoid duplication
async def add_or_update_genres_for_title(conn, title_id, tmdb_genres):
    if not tmdb_genres:
        return

    tmdb_ids = [genre['id'] for genre in tmdb_genres]
    placeholders = ','.join(['%s'] * len(tmdb_ids))
    genre_query = f"""
        SELECT tmdb_genre_id, genre_id
        FROM genres
        WHERE tmdb_genre_id IN ({placeholders})
    """
    result = await aiomysql_conn_execute(conn, genre_query, tmdb_ids, use_dictionary=True)
    genre_ids = {row['tmdb_genre_id']: row['genre_id'] for row in result}

    # Remove old associations
    await aiomysql_conn_execute(conn, "DELETE FROM title_genres WHERE title_id = %s", (title_id,))

    # Prepare new associations
    values = [
        (title_id, genre_ids[genre['id']])
        for genre in tmdb_genres if genre['id'] in genre_ids
    ]
    if values:
        placeholders = ','.join(['(%s, %s)'] * len(values))
        flat_values = [item for pair in values for item in pair]
        insert_query = f"""
            INSERT INTO title_genres (title_id, genre_id)
            VALUES {placeholders}
        """
        await aiomysql_conn_execute(conn, insert_query, flat_values)


# Seperate function to handle the api request
async def get_video_name(youtube_id):
    print(f"Querying Youtube API v3: {youtube_id}")
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        'part': 'snippet',
        'id': youtube_id,
        'key': os.getenv("YOUTUBE_API_KEY")
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if 'items' in data and len(data['items']) > 0:
            return data['items'][0]['snippet']['title']
    return None  # Return None if the video was not found


# Used for the tvs and movies to add the trailers to a title avoid duplication
async def add_or_update_trailers_for_title(conn, title_id, youtube_ids):
    if not youtube_ids:
        return  # No youtube ids to add
    
    # Construct the INSERT query to add new trailers
    values = []
    params = []
    
    # Set the first trailer in the list as the default if no default exists yet
    is_default = True  # Assume the first trailer is the default for simplicity
    
    # Get video names from YouTube API
    video_names = []
    for youtube_id in youtube_ids:
        video_name = await get_video_name(youtube_id)
        video_names.append(video_name if video_name else 'Unknown')  # Default to 'Unknown' if no name found
    
    for i, youtube_id in enumerate(youtube_ids):
        values.append("(%s, %s, %s, %s)")  # Adding a video_name to the insert query
        params.extend([youtube_id, title_id, video_names[i], is_default])
        is_default = False  # Set is_default to False for the rest of the trailers
    
    # Insert the new trailers (assuming they don't exist already)
    insert_query = f"""
        INSERT INTO title_trailers (youtube_id, title_id, video_name, is_default)
        VALUES {', '.join(values)}
        ON DUPLICATE KEY UPDATE
            is_default = VALUES(is_default), video_name = VALUES(video_name);
    """
    
    # Execute the insert query with params
    await aiomysql_conn_execute(conn, insert_query, params)


# Used for both tv and movies the same way to unify with a function
# Get just the stuff that we can't get from tmdb
async def get_extra_info_from_omdb(conn, imdb_id, title_id):
    if imdb_id and title_id:
        omdb_result = await query_omdb(imdb_id)
        # print(omdb_result)

        omdb_insert_query = """
            UPDATE titles
            SET imdb_vote_average = %s,
                imdb_vote_count = %s,
                awards = %s
            WHERE title_id = %s
        """
        
        imdb_rating = omdb_result.get("imdbRating")
        imdb_votes = omdb_result.get("imdbVotes")
        awards = omdb_result.get("Awards")

        omdb_insert_params = (
            imdb_rating if imdb_rating and imdb_rating != "N/A" else 0,
            imdb_votes.replace(",", "") if imdb_votes and imdb_votes != "N/A" else None,
            awards if awards and awards != "N/A" else None,
            title_id
        )
        await aiomysql_conn_execute(conn, omdb_insert_query, omdb_insert_params)


def format_FI_age_rating(rating):
    rating = rating.upper()
    if rating == 'S':
        return rating
    if 'K' not in rating:
        rating = 'K' + rating
    if '-' not in rating:
        rating = rating[:1] + '-' + rating[1:]
    return rating


# Functions for the actual adding/updating of a movie or a tv-show
async def add_or_update_movie_title(
    conn,
    tmdb_id: int, 
    update_title_info=True, 
    update_title_images=False
):
    if update_title_info:
        # Get the data from TMDB
        movie_title_info = await query_tmdb(f"/movie/{tmdb_id}", {
            "append_to_response": "images,releases,videos",
            "include_image_language": "en,null",
        })

        # Retrieve the age rating
        movie_title_age_rating = None
        us_movie_title_age_rating = None
        for release in movie_title_info['releases']['countries']:
            if release['iso_3166_1'] == 'FI' and release['certification']:
                movie_title_age_rating = format_FI_age_rating(release['certification'])
                break
            elif release['iso_3166_1'] == 'US' and release['certification'] != '':
                us_movie_title_age_rating = release['certification']

        if movie_title_age_rating == None:
            if us_movie_title_age_rating:
                movie_title_age_rating = us_movie_title_age_rating
            else:
                movie_title_age_rating = None

        # Retrieve the youtube trailer key
        movie_title_trailers_youtube_ids = []
        for video in movie_title_info["videos"]["results"]:
            if video["site"] == "YouTube" and video["type"] == "Trailer":
                movie_title_trailers_youtube_ids.append(video["key"])

        # Retrieve the production countries and just add them up to a list
        movie_title_production_countries = ""
        for country in movie_title_info.get('production_countries', []):
            if movie_title_production_countries:
                movie_title_production_countries += ", "
            movie_title_production_countries += country["name"]

        # Generate params
        params = (
            movie_title_info.get('id'),
            movie_title_info.get('imdb_id'),
            'movie',
            movie_title_info.get('title'),
            movie_title_info.get('original_title'),
            movie_title_info.get('tagline'),
            movie_title_info.get('vote_average'),   # These are from tmdb so don't add tmdb_
            movie_title_info.get('vote_count'),     # These are from tmdb so don't add tmdb_
            movie_title_info.get('overview'),
            movie_title_info.get('poster_path'),
            movie_title_info.get('backdrop_path'),
            movie_title_info.get('runtime'),
            movie_title_info.get('release_date'),
            movie_title_info.get('original_language'),
            movie_title_age_rating,
            movie_title_info.get('revenue'),
            movie_title_info.get('budget'),
            movie_title_production_countries
        )

        # Generate placeholders based on params
        placeholders = ', '.join(['%s'] * len(params))
        # Create query and place placholders in it
        query = f"""
            INSERT INTO titles (
                tmdb_id,
                imdb_id,
                type,
                name,
                name_original,
                tagline,
                tmdb_vote_average,
                tmdb_vote_count,
                overview,
                backup_poster_url,
                backup_backdrop_url,
                movie_runtime,
                release_date,
                original_language,
                age_rating,
                revenue,
                budget,
                production_countries
            )
            VALUES ({placeholders})
            AS new(tmdb_id, imdb_id, type, name, name_original, tagline,
                tmdb_vote_average, tmdb_vote_count, overview, backup_poster_url,
                backup_backdrop_url, movie_runtime, release_date, original_language,
                age_rating, revenue, budget, production_countries)
            ON DUPLICATE KEY UPDATE 
                imdb_id = new.imdb_id,
                name = new.name,
                name_original = new.name_original,
                tagline = new.tagline,
                tmdb_vote_average = new.tmdb_vote_average,
                tmdb_vote_count = new.tmdb_vote_count,
                overview = new.overview,
                backup_poster_url = new.backup_poster_url,
                backup_backdrop_url = new.backup_backdrop_url,
                movie_runtime = new.movie_runtime,
                release_date = new.release_date,
                original_language = new.original_language,
                age_rating = new.age_rating,
                revenue = new.revenue,
                budget = new.budget,
                production_countries = new.production_countries;
        """

        # Actual query
        title_id = await aiomysql_conn_execute(conn, query, params, return_lastrowid=True)

        if not title_id or title_id == 0:
            # Retrieve the generated titles id
            title_id = await tmdb_to_title_id(conn, tmdb_id)

        # Handle genres using the seperate function
        await add_or_update_genres_for_title(conn, title_id, movie_title_info.get('genres', []))

        # Handle trailers with the seperate function
        await add_or_update_trailers_for_title(conn, title_id, movie_title_trailers_youtube_ids)

        # OMDB query to get more info
        await get_extra_info_from_omdb(conn, movie_title_info.get('imdb_id'), title_id)

        # Set images for image fetching
        title_images_data = movie_title_info.get('images')
    
    elif update_title_images:
        # Query just for the images if we aren't updating info and just updating images
        title_images_data = await query_tmdb(f"/movie/{tmdb_id}/images", {
            "include_image_language": "en,null",
        })
        # Get the title_id from tmdb id
        title_id = await tmdb_to_title_id(conn, tmdb_id)

    # Store the title related images
    # Handle the replacement check for each image. If we were to check also here it wouldn't automatically update missing images.
    await store_title_images(title_images_data, title_id, update_title_images)
    
    return title_id


async def add_or_update_tv_title(
    conn,
    tmdb_id,
    update_title_info=True, 
    update_title_images=False, 
    update_season_number=0,  # If 0 update all, or if > 0 uses the season number
    update_season_info=False, 
    update_season_images=False
):
    # Check if we are updating any of the actual title's data or just episodes (seasons)
    if update_title_info or update_title_images:
        if update_title_info:
            # Get the data from tmdb
            tv_title_info = await query_tmdb(f"/tv/{tmdb_id}", {
                "append_to_response": "external_ids,images,content_ratings,videos", 
                "include_image_language": "en,null"
            })

            # - - - TITLE - - - 

            # Retrieve the age rating
            tv_title_age_rating = None
            us_tv_title_age_rating = None
            for release in tv_title_info['content_ratings']['results']:
                if release['iso_3166_1'] == 'FI' and release['rating']:
                    tv_title_age_rating = format_FI_age_rating(release['rating'])
                    break
                elif release['iso_3166_1'] == 'US' and release['rating']:
                    us_tv_title_age_rating = release['rating']

            if tv_title_age_rating == None:
                if us_tv_title_age_rating:
                    tv_title_age_rating = us_tv_title_age_rating
                else:
                    tv_title_age_rating = ""

            # Retrieve the youtube trailer key
            tv_title_trailers_youtube_ids = []
            for video in tv_title_info["videos"]["results"]:
                if video["site"] == "YouTube" and video["type"] == "Trailer":
                    tv_title_trailers_youtube_ids.append(video["key"])

            # Retrieve the imdb id seperately since it's not part of the base query like in movies
            imdb_id = tv_title_info.get('external_ids', {}).get('imdb_id')

            # Retrieve the production countries and just add them up to a list
            tv_title_production_countries = ""
            for country in tv_title_info.get('production_countries', []):
                if tv_title_production_countries:
                    tv_title_production_countries += ", "
                tv_title_production_countries += country["name"]
                
            tv_title_params = (
                tv_title_info.get('id'),
                imdb_id,
                'tv',
                tv_title_info.get('name'),
                tv_title_info.get('original_name'),
                tv_title_info.get('tagline'),
                tv_title_info.get('vote_average'),  # These are from tmdb so don't add tmdb_
                tv_title_info.get('vote_count'),    # These are from tmdb so don't add tmdb_
                tv_title_info.get('overview'),
                tv_title_info.get('poster_path'),
                tv_title_info.get('backdrop_path'),
                # there's no runtime since its tv
                tv_title_info.get('first_air_date'),
                tv_title_info.get('original_language'),
                tv_title_age_rating,
                # tv_title_info.get('revenue'), # Doesn't seem to exist on tv
                # tv_title_info.get('budget'),  # Doesn't seem to exist on tv
                tv_title_production_countries
            )

            # Generate placeholders based on params
            tv_title_placeholders = ', '.join(['%s'] * len(tv_title_params))
            # Create query and place placholders in it
            tv_title_query = f"""
                INSERT INTO titles (
                    tmdb_id, 
                    imdb_id, 
                    type, 
                    name, 
                    name_original, 
                    tagline, 
                    tmdb_vote_average, 
                    tmdb_vote_count, 
                    overview, 
                    backup_poster_url, 
                    backup_backdrop_url, 
                    release_date,
                    original_language,
                    age_rating,
                    production_countries
                )
                VALUES ({tv_title_placeholders}) AS new
                ON DUPLICATE KEY UPDATE 
                    imdb_id = new.imdb_id,
                    name = new.name,
                    name_original = new.name_original,
                    tagline = new.tagline,
                    tmdb_vote_average = new.tmdb_vote_average,
                    tmdb_vote_count = new.tmdb_vote_count,
                    overview = new.overview,
                    backup_poster_url = new.backup_poster_url,
                    backup_backdrop_url = new.backup_backdrop_url,
                    release_date = new.release_date,
                    original_language = new.original_language,
                    age_rating = new.age_rating,
                    production_countries = new.production_countries;
            """

            # Set id fetch to False since it often fails
            title_id = await aiomysql_conn_execute(conn, tv_title_query, tv_title_params, return_lastrowid=True)

            if not title_id or title_id == 0:
                # Retrieve the generated titles id
                title_id = await tmdb_to_title_id(conn, tmdb_id)

            # Handle genres using the function
            await add_or_update_genres_for_title(conn, title_id, tv_title_info.get('genres', []))

            # Handle trailers with the seperate function
            await add_or_update_trailers_for_title(conn, title_id, tv_title_trailers_youtube_ids)

            # OMDB query to get more info
            await get_extra_info_from_omdb(conn, imdb_id, title_id)


            # - - - SEASONS - - - 
            # The Season data comes automatically from the tv-shows title query so these are handled with the update_title_data
            tv_seasons_params = []
            season_images_data = {
                "title_id": title_id,
                "seasons": []
            }

            for season in tv_title_info.get('seasons', []):
                if season.get('season_number') == 0:  # Skip season 0 (specials)
                    continue
                
                # Add season data for MySQL insert
                tv_seasons_params.append((
                    title_id,
                    season.get('season_number'),
                    season.get('name'),
                    season.get('vote_average'),
                    None,  # TMDB does not provide vote_count for seasons
                    season.get('episode_count'),
                    season.get('overview'),
                    season.get('poster_path'),
                ))

                # Add season poster data for image storage
                if season.get("poster_path"):  # Only add if poster exists
                    season_images_data = [
                        {"season_number": season["season_number"], "poster_path": season["poster_path"]}
                        for season in tv_title_info.get("seasons", [])
                        if season.get("poster_path")  # Only include valid posters
                    ]

            # Insert into the database if there are valid seasons
            if tv_seasons_params:
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s)"] * len(tv_seasons_params))
                query = f"""
                    INSERT INTO seasons (
                        title_id, season_number, season_name, tmdb_vote_average, tmdb_vote_count,
                        episode_count, overview, backup_poster_url
                    )
                    VALUES {placeholders}
                    AS new(title_id, season_number, season_name, tmdb_vote_average, tmdb_vote_count,
                        episode_count, overview, backup_poster_url)
                    ON DUPLICATE KEY UPDATE
                        season_name = new.season_name,
                        tmdb_vote_average = new.tmdb_vote_average,
                        tmdb_vote_count = new.tmdb_vote_count,
                        episode_count = new.episode_count,
                        overview = new.overview,
                        backup_poster_url = new.backup_poster_url;
                """
                flat_values = [item for sublist in tv_seasons_params for item in sublist]
                await aiomysql_conn_execute(conn, query, flat_values)
                        
            # Set the images for 
            title_images_data = tv_title_info.get('images')


        # If we aren't updating info and just updating images
        elif update_title_images:

            # Get the title_id with the tmdb_id from mysql
            title_id = await tmdb_to_title_id(conn, tmdb_id)

            # query just for the images and and general data for seasons images 
            tv_title_info = await query_tmdb(f"/tv/{tmdb_id}", {
                "append_to_response": "images", 
                "include_image_language": "en,null"
            })

            # Title images data
            title_images_data = tv_title_info.get('images')

            # Season images data
            season_images_data = {
                "title_id": title_id,
                "seasons": []
            }
            for season in tv_title_info.get('seasons', []):
                if season.get('season_number') == 0:  # Skip season 0 (specials)
                    continue
                elif season.get("poster_path"):  # Only add if poster exists
                    season_images_data = [
                        {"season_number": season["season_number"], "poster_path": season["poster_path"]}
                        for season in tv_title_info.get("seasons", [])
                        if season.get("poster_path")  # Only include valid posters
                    ]

        # Do not check for the update_title_images since it's handled in the download image function.
        # Instead just run them when ever anything is updated in the titles data with the parameter given to it.

        # Setup the title images to download in the background
        await store_title_images(title_images_data, title_id, update_title_images)
        # Setup the season images to download in the background
        await store_season_images(season_images_data, title_id, update_title_images)
    
    # Else if we didn't run any of the title related code get the title_id for episodes here
    elif update_season_info or update_season_images:
        title_id = await tmdb_to_title_id(conn, tmdb_id)

        # Get the seasons from mysql since we are updating a specific thing that we already have instead of the whole thing for TMDB
        seasons_query = """
            SELECT s.season_number
            FROM seasons s
            JOIN titles t
            ON s.title_id = t.title_id
            WHERE t.title_id = %s
        """
        seasons_result = await aiomysql_conn_execute(conn, seasons_query, (title_id,), use_dictionary=False)

        # Generate a fake tv_title_info from it so that the episodes works with it
        # Create a list of dictionaries with season_number by accessing the tuple element
        tv_title_info = {
            "seasons": [{"season_number": season[0]} for season in seasons_result]
        }
    
    # Else just return since we aren't modifying anything
    else:
        raise HTTPException(status_code=400, detail=f"The function is set to do nothing since all the options are disabled.")

    # - - - EPISODES - - - 
    # This is where we have the seperation between the "title" and "seasons". It's confusing since the seasons data comes form the
    # title's query and the episodes from the seasons query.

    # Do not run any of the episode stuff if we aren't updating any of it.
    if update_season_info or update_season_images:
        # Fetch season IDs from the database
        season_id_query = "SELECT season_id, season_number FROM seasons WHERE title_id = %s"
        season_id_values = await aiomysql_conn_execute(conn, season_id_query, (title_id,), use_dictionary=False)
        season_id_map = {row[1]: row[0] for row in season_id_values}

        # Prepare list of tuples for bulk insertion
        tv_episodes_params = []
        episode_images_data = []

        for season in tv_title_info.get("seasons", []):
            season_number = season.get("season_number")
            season_id = season_id_map.get(season_number)

            if season_id and (season_number == update_season_number or update_season_number == 0):
                season_info = await query_tmdb(f"/tv/{tmdb_id}/season/{season_number}", {})

                for episode in season_info.get("episodes", []):
                    if update_season_info:
                        tv_episodes_params.append((
                            season_id,
                            title_id,
                            episode.get("episode_number"),
                            episode.get("name"),
                            episode.get("vote_average"),
                            episode.get("vote_count"),
                            episode.get("overview"),
                            episode.get("still_path"),
                            episode.get("air_date"),
                            episode.get("runtime")
                        ))

                    # Collect episode images for downloading
                    # We do not check for update_season_images because they should be autofilled with update_season_info
                    if episode.get("still_path"):
                        episode_images_data.append({
                            "season_number": season_number,
                            "episode_number": episode.get("episode_number"),
                            "still_path": episode.get("still_path")
                        })

        if tv_episodes_params:
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(tv_episodes_params))
            query = f"""
                INSERT INTO episodes (
                    season_id, title_id, episode_number, episode_name, tmdb_vote_average,
                    tmdb_vote_count, overview, backup_still_url, air_date, runtime
                )
                VALUES {placeholders}
                AS new(season_id, title_id, episode_number, episode_name, tmdb_vote_average,
                    tmdb_vote_count, overview, backup_still_url, air_date, runtime)
                ON DUPLICATE KEY UPDATE
                    episode_name = new.episode_name,
                    tmdb_vote_average = new.tmdb_vote_average,
                    tmdb_vote_count = new.tmdb_vote_count,
                    overview = new.overview,
                    backup_still_url = new.backup_still_url,
                    air_date = new.air_date,
                    runtime = new.runtime;
            """
            flat_values = [item for sublist in tv_episodes_params for item in sublist]
            await aiomysql_conn_execute(conn, query, flat_values)

        # Setup the episode images to download in the background
        await store_episode_images(episode_images_data, title_id, update_season_images)

    # Finally return the title_id for later use
    return title_id


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


async def convert_season_or_episode_id_to_title_id(conn, season_id=None, episode_id=None):
    if season_id:
        # Get the title_id from the season_id
        get_title_id_query = """
            SELECT title_id
            FROM seasons
            WHERE season_id = %s
        """
        result = await aiomysql_conn_execute(conn, get_title_id_query, (season_id,), use_dictionary=False)
        title_id = result[0][0] if result else None
    elif episode_id:
        # Get the title_id from the episode_id
        get_title_id_query = """
            SELECT title_id
            FROM episodes
            WHERE episode_id = %s
        """
        result = await aiomysql_conn_execute(conn, get_title_id_query, (episode_id,), use_dictionary=False)
        title_id = result[0][0] if result else None
    else:
        title_id = None

    return title_id


async def keep_title_watch_count_up_to_date(conn, user_id, title_id=None, season_id=None, episode_id=None):
    if not title_id:
        title_id = await convert_season_or_episode_id_to_title_id(conn, season_id, episode_id)

    if title_id:
        min_watch_count_query = """
            SELECT MIN(COALESCE(ued.watch_count, 0))
            FROM episodes e
            LEFT JOIN user_episode_details ued ON e.episode_id = ued.episode_id AND ued.user_id = %s
            WHERE e.title_id = %s
        """
        result = await aiomysql_conn_execute(conn, min_watch_count_query, (user_id, title_id), use_dictionary=False)
        min_watch_count = result[0][0] if result else 0
        print(min_watch_count)
        update_title_watch_count_query = """
            INSERT INTO user_title_details (user_id, title_id, watch_count)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE watch_count = %s
        """
        await aiomysql_conn_execute(conn, update_title_watch_count_query, (user_id, title_id, min_watch_count, min_watch_count))


def get_updated_user_title_data(user_id: int, title_id: int):
    try:
        result = {}

        # Query for title's watch_count (for movie or TV show)
        title_query = """
            SELECT utd.watch_count
            FROM user_title_details utd
            WHERE utd.user_id = %s AND utd.title_id = %s
        """
        title_info = query_mysql(title_query, (user_id, title_id))

        if title_info:
            result["title_id"] = title_id
            result["watch_count"] = title_info[0][0]

            # Query for episodes if it's a TV show
            episode_query = """
                SELECT e.episode_id, e.episode_name, ued.watch_count
                FROM episodes e
                LEFT JOIN user_episode_details ued 
                    ON e.episode_id = ued.episode_id AND ued.user_id = %s
                WHERE e.title_id = %s
            """
            episode_info = query_mysql(episode_query, (user_id, title_id))
            result["episodes"] = [
                {
                    "episode_id": episode[0],
                    "episode_name": episode[1],
                    "watch_count": episode[2]
                }
                for episode in episode_info
            ]
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching updated data: {str(e)}")


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


async def check_collection_ownership(conn, collection_id: int, user_id: int):
    query = """
        SELECT 1 FROM user_collection
        WHERE collection_id = %s AND user_id = %s
    """
    result = await aiomysql_conn_execute(conn, query, (collection_id, user_id))
    if not result:
        raise HTTPException(status_code=403, detail="You do not own this collection.")


# ------------ Titles ------------

# Allows both title_id and tmdb_id, while preferring title_id
@router.post("/titles")
async def add_user_title(data: dict):
    async with get_aiomysql_conn() as conn:
        user_id = await validate_session_key_conn(conn, data.get("session_key"))

        title_id = data.get("title_id")
        tmdb_id = data.get("tmdb_id")

        # Prefer title_id if given, check if it exists
        if title_id:
            check_query = "SELECT 1 FROM titles WHERE title_id = %s"
            exists = await aiomysql_conn_execute(conn, check_query, (title_id,), use_dictionary=False)
            if not exists:
                title_id = None  # fallback to tmdb_id logic

        # If no valid title_id, try getting it via tmdb_id
        if not title_id and tmdb_id:
            check_query = "SELECT title_id FROM titles WHERE tmdb_id = %s"
            result = await aiomysql_conn_execute(conn, check_query, (tmdb_id,), use_dictionary=False)
            if result:
                title_id = result[0][0]
            else:
                title_type = str(data.get("title_type", "")).lower()
                if title_type not in {"movie", "tv"}:
                    raise HTTPException(status_code=400, detail="Invalid title_type value!")

                if title_type == "movie":
                    title_id = await add_or_update_movie_title(conn, tmdb_id)
                else:
                    title_id = await add_or_update_tv_title(conn, tmdb_id, update_title_info=True, update_season_info=True, update_season_number=0)

        if not title_id:
            raise HTTPException(status_code=400, detail="Missing or invalid title_id/tmdb_id")

        link_user_query = """
            INSERT INTO user_title_details (user_id, title_id)
            VALUES (%s, %s)
        """
        await aiomysql_conn_execute(conn, link_user_query, (user_id, title_id))

        return {
            "title_id": title_id,
            "message": 'Title added successfully to your watchlist!'
        }


@router.delete("/titles/{title_id}")
async def remove_user_title(title_id: int, data: dict):
    conn = await aiomysql_connect()
    try:
        user_id = await validate_session_key_conn(conn, data.get("session_key"))

        remove_query = """
            DELETE FROM user_title_details
            WHERE user_id = %s AND title_id = %s
        """
        await aiomysql_conn_execute(conn, remove_query, (user_id, title_id))

        remove_episodes_query = """
            DELETE user_episode_details
            FROM user_episode_details
            JOIN episodes ON user_episode_details.episode_id = episodes.episode_id
            WHERE user_episode_details.user_id = %s AND episodes.title_id = %s
        """
        await aiomysql_conn_execute(conn, remove_episodes_query, (user_id, title_id))

        return {
            "success": True,
            "message": 'Title removed from your watchlist successfully!'
        }

    finally:
        conn.close()


@router.put("/titles/{title_id}")
async def update_title(title_id: int, data: dict):
    async with get_aiomysql_conn() as conn:

        title_type = data.get("title_type")

        if not title_id or not title_type:
            raise HTTPException(status_code=422, detail="Missing 'title_id' or 'title_type'.")

        # The "add_or_update_movie_title" uses tmdb_id for reasons so get it with the title_id
        get_tmdb_id_query = """
            SELECT tmdb_id
            FROM titles
            WHERE title_id = %s
        """
        tmdb_id_result = await aiomysql_conn_execute(conn, get_tmdb_id_query, (title_id,), use_dictionary=False)
        tmdb_id = tmdb_id_result[0][0]
        # Check what we are updating and if not given set to default values
        # These are for both so get here.
        update_title_info = data.get("update_title_info", True)
        update_title_images = data.get("update_title_images", False)

        if title_type == "movie":
            await add_or_update_movie_title(conn, tmdb_id, update_title_info, update_title_images)

        elif title_type == "tv":
            # These are tv specific so get only here
            update_season_number = data.get("update_season_number", 0)
            update_season_info = data.get("update_season_info", False)
            update_season_images = data.get("update_season_images", False)

            await add_or_update_tv_title(conn, tmdb_id, update_title_info, update_title_images, update_season_number, update_season_info, update_season_images)
        else:
            raise HTTPException(status_code=422, detail="Invalid 'title_type'. Must be 'movie' or 'tv'.")

        return {"message": "Title information updated successfully."}


@router.put("/titles/{title_id}/notes")
async def save_user_title_notes(title_id: int, data: dict):
    conn = await aiomysql_connect()
    try:
        # Validate the session key
        user_id = await validate_session_key_conn(conn, data.get("session_key"))
        
        notes = data.get("notes")
        
        # Remove title from user's watch list
        save_notes_query = """
            UPDATE user_title_details
            SET notes = %s
            WHERE user_id = %s AND title_id = %s
        """
        await aiomysql_conn_execute(conn, save_notes_query, (notes, user_id, title_id))

        return {"message": "Notes updated successfully!"}
    
    finally:
        conn.close()
    

# Could be more restful by giving a value to set to, but that's for later me.
@router.post("/titles/{title_id}/favourite/toggle")
async def toggle_title_favourite(title_id: int, data: dict):
    try:
        # Validate the session key
        conn = await aiomysql_connect()
        user_id = await validate_session_key_conn(conn, data.get("session_key"))
        
        # Remove title from user's watch list
        save_notes_query = """
            INSERT INTO user_title_details (user_id, title_id, favourite)
            VALUES (%s, %s, NOT favourite)
            ON DUPLICATE KEY UPDATE favourite = NOT favourite
        """
        await aiomysql_conn_execute(conn, save_notes_query, (user_id, title_id))

        return {"message": "Favourite status toggled successfully!"}
    
    finally:
        conn.close()


@router.put("/titles/{title_id}/watch_count")
async def update_title_watch_count(title_id: int, data: dict):
    async with get_aiomysql_conn() as conn:
        user_id = await validate_session_key_conn(conn, data.get("session_key"))

        watch_count = data.get("watch_count")
        if not isinstance(watch_count, int) or watch_count < 0:
            raise HTTPException(status_code=400, detail="watch_count must be a non-negative integer")

        title_type_result = await aiomysql_conn_execute(
            conn, "SELECT type FROM titles WHERE title_id = %s", (title_id,), use_dictionary=False
        )
        if not title_type_result:
            raise HTTPException(status_code=404, detail="Title not found")

        title_type = title_type_result[0][0]

        if title_type == "movie":
            query = """
                INSERT INTO user_title_details (user_id, title_id, watch_count)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count)
            """
            await aiomysql_conn_execute(conn, query, (user_id, title_id, watch_count))

        elif title_type == "tv":
            query = """
                INSERT INTO user_episode_details (user_id, episode_id, watch_count)
                SELECT %s, episode_id, %s
                FROM episodes
                WHERE title_id = %s
                ON DUPLICATE KEY UPDATE watch_count = VALUES(watch_count)
            """
            await aiomysql_conn_execute(conn, query, (user_id, watch_count, title_id))
            await keep_title_watch_count_up_to_date(conn, user_id, title_id=title_id)

        else:
            raise HTTPException(status_code=400, detail="Invalid title type")

        return {"message": "Watch count updated!"}


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
        await aiomysql_conn_execute(conn, query, (user_id, watch_count, season_id))
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
        await aiomysql_conn_execute(conn, query, (user_id, episode_id, watch_count))
        await keep_title_watch_count_up_to_date(conn, user_id, episode_id=episode_id)

        conn.close()

        return {"message": "Episode watch count updated!"}

        # updated_data = get_updated_user_title_data(user_id, title_id)
        # return {"message": "Episode watch count updated!", "updated_data": updated_data}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.get("/titles/cards")
async def get_title_cards(
    session_key: str = Query(...),
    title_count: int = None,
    # Optional sorting
    sort_by: str = None,
    direction: str = None,
    # Optional filters
    title_type: str = Query(None, regex="^(movie|tv)$"),  
    watched: bool = None,
    favourite: bool = None,
    released: bool = None,
    started: bool = None,
):

    # Connect, get user_id and validate session key
    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, session_key, False)

    # Base query
    get_titles_query = """
        SELECT 
            t.title_id, 
            t.name, 
            t.tmdb_vote_average, 
            t.tmdb_vote_count, 
            t.movie_runtime, 
            utd.watch_count, 
            t.type, 
            t.release_date,
            t.backup_poster_url,
            (SELECT COUNT(season_id) FROM seasons WHERE title_id = t.title_id) AS season_count,
            (SELECT COUNT(episode_id) FROM episodes WHERE title_id = t.title_id) AS episode_count,
            utd.favourite,
            GREATEST(COALESCE(utd.last_updated, '1970-01-01'), 
                    COALESCE(MAX(ued.last_updated), '1970-01-01')) AS latest_updated,
            (
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
            ) IS NOT NULL AS new_episodes
        FROM 
            user_title_details utd
        JOIN 
            titles t ON utd.title_id = t.title_id
        LEFT JOIN 
            seasons s ON s.title_id = t.title_id
        LEFT JOIN 
            episodes e ON e.season_id = s.season_id
        LEFT JOIN 
            user_episode_details ued ON ued.user_id = utd.user_id AND ued.episode_id = e.episode_id
        WHERE 
            utd.user_id = %s
    """

    query_params = [user_id]

    # Filter by category if provided
    if title_type:
        get_titles_query += " AND t.type = %s"
        query_params.append(title_type.lower())  # Ensure correct ENUM value

    # Filter by watched status if provided
    if watched is True:
        get_titles_query += " AND utd.watch_count >= 1"
    elif watched is False:
        get_titles_query += " AND utd.watch_count <= 0"

    # Filter by favourite status if provided
    if favourite is True:
        get_titles_query += " AND utd.favourite = TRUE"
    elif favourite is False:
        get_titles_query += " AND utd.favourite = FALSE"

    # Filter by release date if provided
    if released is True:
        get_titles_query += " AND t.release_date <= CURDATE()"
    elif released is False:
        get_titles_query += " AND t.release_date > CURDATE()"

    # Filter by whether the TV show has been started
    if started is True:
        get_titles_query += """
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
        get_titles_query += """
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

    # Grouping clause
    get_titles_query += """
        GROUP BY
            t.title_id, 
            t.name, 
            t.tmdb_vote_average, 
            t.tmdb_vote_count, 
            t.movie_runtime, 
            utd.watch_count, 
            t.type, 
            t.release_date,
            t.backup_poster_url,
            utd.favourite
    """

    # Use the direction if provided
    direction = direction.upper() if direction else "DESC"

    # Add sorting if provided
    if sort_by == "release_date":
        get_titles_query += f" ORDER BY t.release_date {direction}"
    elif sort_by == "last_watched":
        get_titles_query += f" ORDER BY latest_updated {direction}"
    else:
        get_titles_query += f" ORDER BY t.tmdb_vote_average {direction}"


    # Add the limit
    if title_count is None:
        title_count = 12

    get_titles_query += " LIMIT %s"
    query_params.append(title_count)

    # Execute query
    results = await aiomysql_conn_execute(conn, get_titles_query, tuple(query_params), use_dictionary=False)

    conn.close()

    # Format results as objects with relevant fields
    formatted_results = []
    for row in results:
        # Base title data to which the rest is added to
        title_data = {
            "title_id": row[0],
            "name": row[1],
            "vote_average": row[2],
            "vote_count": row[3],
            "movie_runtime": row[4],
            "watch_count": row[5],
            "type": row[6],
            "release_date": row[7],
            "backup_poster_url": row[8],
            "season_count": row[9],
            "episode_count": row[10],
            "favourite": row[11],
            # Don't give the latest_updated since not needed
            "new_episodes": row[13],
        }

        formatted_results.append(title_data)

    return {"titles": formatted_results}


@router.get("/titles")
async def list_titles(
    session_key: str = Query(...),
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
    title_limit: Optional[int] = None,
):
    
    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, session_key, guest_lock=False)

    title_limit = await fetch_user_settings(conn, user_id, 'list_all_titles_load_limit') or 30

    query, query_params = build_titles_query(
        user_id, title_type, watched, favourite, released, started, all_titles, 
        search_term, collection_id, sort_by, direction, offset, title_limit
    )

    results = await aiomysql_conn_execute(conn, query, tuple(query_params), use_dictionary=True)

    conn.close()

    has_more = len(results) > title_limit
    results = results[:title_limit]

    for row in results:
        row["genres"] = row["genres"].split(", ") if row["genres"] else []

    return {
        "titles": results,
        "has_more": has_more,
        "offset": offset,
    }


@router.get("/titles/{title_id}")
async def get_title_info(
    title_id: int,
    session_key: str = Query(...),
):
    # Setup connection
    conn = await aiomysql_connect()

    # Get user_id and validate session key
    user_id = await validate_session_key_conn(conn, session_key, False)

    # Combined title, collections, and trailers query
    get_titles_query = """
        SELECT 
            t.*, 
            utd.watch_count, 
            utd.notes, 
            utd.favourite, 
            utd.last_updated AS user_title_last_updated,
            utd.title_id IS NOT NULL AS in_watch_list,
            GROUP_CONCAT(DISTINCT g.genre_name ORDER BY g.genre_name SEPARATOR ', ') AS genres,
            -- Subquery for collections
            (SELECT JSON_ARRAYAGG(
                JSON_OBJECT('collection_id', uc.collection_id, 'name', uc.name, 'description', uc.description)
            ) FROM user_collection uc
            INNER JOIN collection_title ct ON ct.collection_id = uc.collection_id
            WHERE ct.title_id = t.title_id AND uc.user_id = %s) AS collections,
            -- Subquery for trailers
            (SELECT JSON_ARRAYAGG(
                JSON_OBJECT('trailer_key', youtube_id, 'video_name', video_name, 'is_default', is_default)
            ) FROM title_trailers
            WHERE title_id = t.title_id) AS trailers
        FROM titles t
        LEFT JOIN user_title_details utd ON utd.title_id = t.title_id AND utd.user_id = %s
        LEFT JOIN title_genres tg ON t.title_id = tg.title_id
        LEFT JOIN genres g ON tg.genre_id = g.genre_id
        WHERE t.title_id = %s
        GROUP BY t.title_id, utd.watch_count, utd.notes, utd.favourite, utd.last_updated, utd.title_id;
    """
    title_query_results = await aiomysql_conn_execute(conn, get_titles_query, (user_id, user_id, title_id))
    if not title_query_results:
        raise HTTPException(status_code=404, detail="The title doesn't exist.")
    title_data = title_query_results[0]

    # Add custom links and other properties
    custom_links = custom_combined_links(title_data["name"])

    # Final assembled data
    title_info = {
        **title_data,
        "genres": title_data["genres"].split(", ") if title_data["genres"] else [],
        "collections": json.loads(title_data["collections"]) if title_data["collections"] else [],
        "backdrop_image_count": get_backdrop_count(title_data["title_id"]),
        "logo_file_type": get_logo_type(title_data["title_id"]),
        "watch_now_links": custom_links["links"],
        "trailers": json.loads(title_data["trailers"]) if title_data["trailers"] else [],
    }

    # Query and append extra tv-series related info to title_info
    if title_info["type"] == "tv":
        # Get seasons
        get_seasons_query = """
            SELECT 
                season_id, 
                season_number, 
                season_name, 
                tmdb_vote_average, 
                tmdb_vote_count, 
                episode_count, 
                overview, 
                backup_poster_url
            FROM seasons
            WHERE title_id = %s
            ORDER BY CASE WHEN season_number = 0 THEN 999 ELSE season_number END
        """
        seasons = await aiomysql_conn_execute(conn, get_seasons_query, (title_id,))

        # Get Episodes
        get_episodes_query = """
            SELECT 
                e.season_id, 
                e.episode_id, 
                e.episode_number, 
                e.episode_name, 
                e.tmdb_vote_average, 
                e.tmdb_vote_count, 
                e.overview, 
                e.backup_still_url, 
                e.air_date, 
                e.runtime, 
                COALESCE(ued.watch_count, 0) AS watch_count
            FROM episodes e
            LEFT JOIN user_episode_details ued 
                ON e.episode_id = ued.episode_id 
                AND ued.user_id = %s
            WHERE e.title_id = %s
            ORDER BY e.season_id, e.episode_number;
        """
        episodes = await aiomysql_conn_execute(conn, get_episodes_query, (user_id, title_id))

        # Get episodes links
        episode_links = custom_links["episodes"]

        # Map out and combine data
        season_map = {s["season_id"]: {**s, "episodes": []} for s in seasons}
        season_number_map = {s["season_id"]: s["season_number"] for s in seasons}

        for episode in episodes:
            season_id = episode["season_id"]
            season_number = season_number_map.get(season_id)
            if season_id in season_map:
                episode_key = f"Episode {episode['episode_number']:02d}"
                season_key = f"Season {season_number:02d}"
                
                if season_key in episode_links and episode_key in episode_links[season_key]:
                    episode["watch_now_links"] = [episode_links[season_key][episode_key]]
                
                season_map[season_id]["episodes"].append(episode)

        title_info["seasons"] = list(season_map.values())
    
    conn.close()

    return {"title_info": title_info}


@router.get("/titles/{title_id}/collections")
async def list_collections(
    title_id: str,
    session_key: str = Query(...),
):
    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, session_key)

    query = """
        SELECT
            uc.collection_id,
            uc.name,
            uc.description,
            uc.parent_collection_id,
            CASE
                WHEN ct.title_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS title_in_collection
        FROM user_collection uc
        LEFT JOIN collection_title ct
            ON uc.collection_id = ct.collection_id AND ct.title_id = %s
        WHERE uc.user_id = %s
        ORDER BY title_in_collection DESC, uc.name ASC
    """
    collections = await aiomysql_conn_execute(conn, query, (title_id, user_id))
    conn.close()

    collection_dict = {c['collection_id']: {**c, 'children': []} for c in collections}
    root_collections = []

    for collection in collection_dict.values():
        parent_id = collection['parent_collection_id']
        if parent_id and parent_id in collection_dict:
            collection_dict[parent_id]['children'].append(collection)
        else:
            root_collections.append(collection)

    return root_collections


# ------------ Collections ------------

@router.post("/collections")
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
    collection_id = await aiomysql_conn_execute(conn, query, (user_id, name, description), return_lastrowid=True)
    conn.close()

    return {
        "message": "Collection created successfully!",
        "collection": {
            'collection_id': collection_id,
            'name': name, 
            'description': description
        }
    }


@router.put("/collections/{collection_id}")
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
    await aiomysql_conn_execute(conn, query, tuple(values))

    return {
        "message": "Collection updated successfully!"
    }


@router.delete("/collections/{collection_id}")
async def delete_collection(collection_id: int, data: dict):
    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    await check_collection_ownership(conn, collection_id, user_id)

    query = """
        DELETE FROM user_collection 
        WHERE user_id = %s AND collection_id = %s
    """
    await aiomysql_conn_execute(conn, query, (user_id, collection_id))
    conn.close()

    return {
        "message": "Collection deleted successfully!"
    }


@router.get("/collections")
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
    collections = await aiomysql_conn_execute(conn, query, (user_id,))

    collection_map = {c['collection_id']: {**c, 'titles': [], 'children': []} for c in collections}

    for collection in collection_map.values():
        query, query_params = build_titles_query(
            user_id,
            title_type=None,
            watched=None,
            favourite=None,
            released=None,
            started=None,
            all_titles=None,
            search_term=None,
            collection_id=collection['collection_id'],
            sort_by='release_date',
            direction='ASC',
            offset=0,
            title_limit=None
        )
        titles = await aiomysql_conn_execute(conn, query, tuple(query_params))
        for row in titles:
            row["genres"] = row["genres"].split(", ") if row["genres"] else []
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


@router.put("/collections/{collection_id}/title/{title_id}")
async def add_title_to_collection(collection_id: int, title_id: int, data: dict):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    await check_collection_ownership(conn, collection_id, user_id)

    query = """
        INSERT INTO collection_title (collection_id, title_id)
        VALUES (%s, %s)
    """
    await aiomysql_conn_execute(conn, query, (collection_id, title_id))
    conn.close()

    return {
        "message": "Title added successfully to the collection!"
    }


@router.delete("/collections/{collection_id}/title/{title_id}")
async def remove_title_from_collection(collection_id: int, title_id: int, data: dict):

    conn = await aiomysql_connect()
    user_id = await validate_session_key_conn(conn, data.get("session_key"))

    await check_collection_ownership(conn, collection_id, user_id)

    query = """
        DELETE FROM collection_title 
        WHERE collection_id = %s AND title_id = %s
    """
    await aiomysql_conn_execute(conn, query, (collection_id, title_id))
    conn.close()

    return {
        "message": "Title removed successfully from the collection!"
    }


# ------------ Mixed ------------

# Acts as a middle man between TMDB search and vue. 
# Adds proper genres and the fact wether the user has added the title or not.
@router.get("/search")
async def watch_list_search(
    session_key: str = Query(...),
    title_category: str = Query(..., regex="^(movie|tv)$"),
    title_name: str = Query(None),
):
    # Validate the session key and retrieve the user ID
    user_id = validate_session_key(session_key, False)

    # Fetch search results from cache or TMDB API
    if title_name:
        title_lower = title_name.lower()

        # Try to get from Redis cache first
        search_results = await get_from_cache(title_lower)
        if search_results is None:
            search_results = query_tmdb(
                f"/search/{title_category}",
                {"query": title_name, "include_adult": False}
            )
            # Store results in Redis cache
            await add_to_cache(title_lower, search_results, timedelta(weeks=1))
        else:
            print(f"Found \"{title_lower}\" from redis. Using it instead of querying TMDB.")
    else:
        raise HTTPException(status_code=400, detail="Title name is required.")

    # Retrieve genre mappings from the database
    genre_query = "SELECT tmdb_genre_id, genre_name FROM genres"
    genre_data = query_mysql(genre_query, ())
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
        title_id_data = query_mysql(title_id_query, (*tmdb_ids,))
        title_id_dict = {row[0]: row[1] for row in title_id_data}

        # Query to get user's watchlist details
        watchlist_query = f"""
            SELECT t.tmdb_id, t.title_id
            FROM user_title_details utd
            JOIN titles t ON utd.title_id = t.title_id
            WHERE utd.user_id = %s AND t.tmdb_id IN ({placeholders})
        """
        watchlist_data = query_mysql(watchlist_query, (user_id, *tmdb_ids))
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

    return search_results


# Used to manually update the genres if they change etc. In the past was ran always on start, but since it ran on all 4 workers the feature was removed. Basically only used if I were to wipe the whole db.
@router.put("/genres")
def update_genres():
    # Fetch movie genres
    movie_genres = query_tmdb("/genre/movie/list", {})
    if movie_genres:
        for genre in movie_genres.get("genres", []):
            genre_id = genre.get("id")
            genre_name = genre.get("name")
            # Check if genre exists in the database
            existing_genre = query_mysql("SELECT * FROM genres WHERE tmdb_genre_id = %s", (genre_id,))
            if not existing_genre:
                # Insert new genre
                query_mysql("INSERT INTO genres (tmdb_genre_id, genre_name) VALUES (%s, %s)", (genre_id, genre_name))
            else:
                # Update genre if name changes
                query_mysql("UPDATE genres SET genre_name = %s WHERE tmdb_genre_id = %s", (genre_name, genre_id))
        print("Movie genres stored!")

    # Fetch TV genres
    tv_genres = query_tmdb("/genre/tv/list", {})
    if tv_genres:
        for genre in tv_genres.get("genres", []):
            genre_id = genre.get("id")
            genre_name = genre.get("name")
            # Check if genre exists in the database
            existing_genre = query_mysql("SELECT * FROM genres WHERE tmdb_genre_id = %s", (genre_id,))
            if not existing_genre:
                # Insert new genre
                query_mysql("INSERT INTO genres (tmdb_genre_id, genre_name) VALUES (%s, %s)", (genre_id, genre_name))
            else:
                # Update genre if name changes
                query_mysql("UPDATE genres SET genre_name = %s WHERE tmdb_genre_id = %s", (genre_name, genre_id))
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
