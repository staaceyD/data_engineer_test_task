import sqlite3
from sqlite3 import Error

from bucket_wraper import get_files, process_files
from constants import (FILES_LIST, SQL_CREATE_APPS_TABLE,
                       SQL_CREATE_FILE_NAMES_TABLE, SQL_CREATE_MOVIES_TABLE,
                       SQL_CREATE_SONGS_TABLE)
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
    c = conn.cursor()
    c.execute("SELECT name FROM file_names WHERE name=?", file_name)
    data = c.fetchall()

    if len(data) == 0:
        sql = ''' INSERT INTO file_names(name)
              VALUES(?) '''
        c.execute(sql, file_name)
        conn.commit()
        return c.lastrowid
    else:
        print("The file is already proccessed, record will not be added")


def create_song_record(conn, song):
    """
    Create a new record into the songs table
    :param conn:
    :param song:
    :return: 
    """
    sql = ''' INSERT INTO songs(artist_name,title,year,release,ingestion_time, file_name_id)
              VALUES(?,?,?,?,?,?) '''
    c = conn.cursor()
    c.execute(sql, song)
    conn.commit()
    return c.lastrowid


def create_movie_record(conn, movie):
    """
    Create a new record into the movies table
    :param conn:
    :param movie:
    :return: 
    """
    sql = ''' INSERT INTO movies(original_title,original_language,budget,is_adult,release_date, original_title_normalized, file_name_id)
              VALUES(?,?,?,?,?,?,?) '''
    c = conn.cursor()
    c.execute(sql, movie)
    conn.commit()
    return c.lastrowid


def create_app_record(conn, app):
    """
    Create a new record into the apps table
    :param conn:
    :param app:
    :return: 
    """
    sql = ''' INSERT INTO apps(name,genre,rating,version,size_bytes, is_awesome, file_name_id)
              VALUES(?,?,?,?,?,?,?) '''
    c = conn.cursor()
    c.execute(sql, app)
    conn.commit()
    return c.lastrowid


def main():
    database = "data_engineer_test_task/types.db"

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, SQL_CREATE_FILE_NAMES_TABLE)
        create_table(conn, SQL_CREATE_SONGS_TABLE)
        create_table(conn, SQL_CREATE_MOVIES_TABLE)
        create_table(conn, SQL_CREATE_APPS_TABLE)
    else:
        print("Error! cannot create the database connection.")

    # create a record
    with conn:
        file_names_list = get_files(FILES_LIST).split("\n")
        for file_name in file_names_list:

            file_name_id = create_file_name_record(conn, (file_name,))

        data = process_files()
        if file_name_id is not None:
            for song in add_song(data, file_name_id):
                create_song_record(conn, song)

            for movie in add_movie(data, file_name_id):
                create_movie_record(conn, movie)

            for app in add_app(data, file_name_id):
                create_app_record(conn, app)
        else:
            print("The file is already proccessed, record will not be added")


if __name__ == '__main__':
    main()
