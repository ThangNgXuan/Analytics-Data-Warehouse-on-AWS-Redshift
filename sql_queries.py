import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
S3_LOG_DATA = config.get("S3","LOG_DATA")
S3_SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_location VARCHAR,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

# apply distributed style (distkey, sortkey)
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                                        songplay_id INT IDENTITY(0,1), 
                                                        start_time BIGINT NOT NULL, 
                                                        user_id INT NOT NULL, 
                                                        level VARCHAR NOT NULL, 
                                                        song_id VARCHAR, 
                                                        artist_id VARCHAR, 
                                                        session_id INT, 
                                                        location VARCHAR NOT NULL, 
                                                        user_agent VARCHAR NOT NULL);
                                                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY, 
                                                        first_name VARCHAR, 
                                                        last_name VARCHAR, 
                                                        gender VARCHAR, 
                                                        level VARCHAR);
                                                        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR NOT NULL PRIMARY KEY, 
                                                        title VARCHAR NOT NULL, 
                                                        artist_id VARCHAR NOT NULL, 
                                                        year int NOT NULL, 
                                                        duration FLOAT NOT NULL);
                                                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR NOT NULL PRIMARY KEY, 
                                                        name VARCHAR NOT NULL, 
                                                        location VARCHAR, 
                                                        latitude FLOAT, 
                                                        longitude FLOAT);
                                                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, 
                                                        hour INT, 
                                                        day INT,
                                                        week INT, 
                                                        month INT, 
                                                        year INT, 
                                                        weekday INT);
                                                        """)
# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
                          iam_role {}
                          json {};
                       """.format(S3_LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH))

staging_songs_copy = ("""copy staging_songs from {}
                          iam_role {} 
                          json 'auto';
                       """.format(S3_SONG_DATA, DWH_ROLE_ARN))
# FINAL TABLES

# casting data type in select query
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        se.ts, 
        se.userId, se.level, ss.song_id, ss.artist_id, 
        se.sessionId, se.location, se.userAgent
    FROM staging_events se 
    INNER JOIN staging_songs ss 
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  ti.start_time,
            extract (hour from ti.start_time) as hour,
            extract (day from ti.start_time) as day,
            extract (week from ti.start_time) as week,
            extract (month from ti.start_time) as month,
            extract (year from ti.start_time) as year,
            extract (weekday from ti.start_time) as weekday     
    FROM (
        SELECT DISTINCT
            timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time
        FROM staging_events
        WHERE page = 'NextSong'
    ) ti
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
