SQL_CREATE_FILE_NAMES_TABLE = """ CREATE TABLE IF NOT EXISTS file_names (
                                        id integer PRIMARY KEY,
                                        name text
                                    ); """

SQL_CREATE_SONGS_TABLE = """ CREATE TABLE IF NOT EXISTS songs (
                                    id integer PRIMARY KEY,
                                    artist_name text,
                                    title text,
                                    year integer,
                                    release text,
                                    ingestion_time text,
                                    file_name_id int,
                                    FOREIGN KEY (file_name_id) REFERENCES file_names (id)
                                ); """

SQL_CREATE_MOVIES_TABLE = """CREATE TABLE IF NOT EXISTS movies (
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

SQL_CREATE_APPS_TABLE = """CREATE TABLE IF NOT EXISTS apps (
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
