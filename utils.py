import re
from datetime import datetime


def add_song(data, file_name_id):
    songs_data = []
    for record in data:
        if record["type"] == "song":
            songs_data.append((record["data"]["artist_name"], record["data"]["title"],
                              record["data"]["year"], record["data"]["release"], str(datetime.now()), file_name_id))
    return songs_data


def add_movie(data, file_name_id):

    movies_data = []
    for record in data:
        if record["type"] == "movie":
            original_title_normalized = re.sub(
                r"\W", " ", record["data"]["original_title"].lower())
            original_title_normalized = re.sub(
                " +", '_', original_title_normalized)
            movies_data.append((record["data"]["original_title"], record["data"]["original_language"], record["data"]
                               ["budget"], record["data"]["is_adult"], record["data"]["release_date"], original_title_normalized, file_name_id))
    return movies_data


def add_app(data, file_name_id):
    apps_data = []

    for record in data:
        if record["type"] == "app":
            is_awesome = True if record["data"]["rating"] >= 4 else False
            apps_data.append((record["data"]["name"], record["data"]["genre"], record["data"]
                             ["rating"], record["data"]["version"], record["data"]["size_bytes"], is_awesome, file_name_id))
    return apps_data
