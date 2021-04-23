import sqlite3
from sqlite3 import Error

from bucket_wraper import get_files, process_files
from config import FILES_LIST
from utils import add_app, add_movie, add_song


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:

        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_file_name_record(conn, file_name):
    """
    Create a new record into the file_names table
    :param conn:
    :param file_name:
    :return: file_name id
    """
    sql = ''' INSERT INTO file_names(name)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, file_name)
    conn.commit()
    return cur.lastrowid


def create_song_record(conn, song):
    """
    Create a new record into the songs table
    :param conn:
    :param song:
    :return: 
    """
    sql = ''' INSERT INTO songs(artist_name,title,year,release,ingestion_time, file_name_id)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, song)
    conn.commit()
    return cur.lastrowid


def create_movie_record(conn, movie):
    """
    Create a new record into the movies table
    :param conn:
    :param movie:
    :return: 
    """
    sql = ''' INSERT INTO movies(original_title,original_language,budget,is_adult,release_date, original_title_normalized, file_name_id)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, movie)
    conn.commit()
    return cur.lastrowid


def create_app_record(conn, app):
    """
    Create a new record into the apps table
    :param conn:
    :param app:
    :return: 
    """
    sql = ''' INSERT INTO apps(name,genre,rating,version,size_bytes, is_awesome, file_name_id)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, app)
    conn.commit()
    return cur.lastrowid


def main():
    database = "data_engineer_test_task/types.db"

    sql_create_file_names_table = """ CREATE TABLE IF NOT EXISTS file_names (
                                        id integer PRIMARY KEY,
                                        name text
                                    ); """

    sql_create_songs_table = """ CREATE TABLE IF NOT EXISTS songs (
                                        id integer PRIMARY KEY,
                                        artist_name text,
                                        title text,
                                        year integer,
                                        release text,
                                        ingestion_time text,
                                        file_name_id int,
                                        FOREIGN KEY (file_name_id) REFERENCES file_names (id)
                                    ); """

    sql_create_movies_table = """CREATE TABLE IF NOT EXISTS movies (
                                    id integer PRIMARY KEY,
                                    original_title text,
                                    original_language text,
                                    budget integer,
                                    is_adult text,
                                    release_date text,
                                    original_title_normalized text,
                                    file_name_id int,
                                    FOREIGN KEY (file_name_id) REFERENCES file_names (id)
                                );"""

    sql_create_apps_table = """CREATE TABLE IF NOT EXISTS apps (
                                    id integer PRIMARY KEY,
                                    name text,
                                    genre text,
                                    rating real,
                                    version text,
                                    size_bytes int,
                                    is_awesome text,
                                    file_name_id int,
                                    FOREIGN KEY (file_name_id) REFERENCES file_names (id)
                                );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, sql_create_file_names_table)
        create_table(conn, sql_create_songs_table)
        create_table(conn, sql_create_movies_table)
        create_table(conn, sql_create_apps_table)
    else:
        print("Error! cannot create the database connection.")

    # create a record
    with conn:
        file_names_list = get_files(FILES_LIST).split("\n")
        for file_name in file_names_list:
            file_name_id = create_file_name_record(conn, (file_name,))

        data = process_files()

        for song in add_song(data, file_name_id):
            create_song_record(conn, song)

        for movie in add_movie(data, file_name_id):
            create_movie_record(conn, movie)

        for app in add_app(data, file_name_id):
            create_app_record(conn, app)


if __name__ == '__main__':
    main()
