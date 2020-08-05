import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


#VARIABLES
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA' )
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplays_table_drop = "DROP TABLE IF EXISTS songplays;"
users_table_drop = "DROP TABLE IF EXISTS users;"
songs_table_drop = "DROP TABLE IF EXISTS songs;"
artists_table_drop = "DROP TABLE IF EXISTS artists;"
times_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR NOT NULL,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT NOT NULL,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR NOT NULL,
        location VARCHAR,
        method VARCHAR NOT NULL,
        page VARCHAR NOT NULL,
        registration NUMERIC,
        sessionId INT NOT NULL,
        song VARCHAR,
        status INT NOT NULL,
        ts NUMERIC NOT NULL,
        userAgent VARCHAR,
        userId INT);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT NOT NULL,
        artist_id VARCHAR NOT NULL,
        artist_latitude VARCHAR,
        artist_longitude NUMERIC,
        artist_location NUMERIC,
        artist_name VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        duration NUMERIC NOT NULL,
        year INT NOT NULL);
""")


songplays_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplays_id IDENTITY(0,1) PRIMARY KEY, 
        start_time BIGINT NOT NULL, 
        user_id INT NOT NULL, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR);
""")

users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY, 
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        gender CHAR(1), 
        level VARCHAR);
""") 

songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year INT, 
        duration REAL NOT NULL);
""")

artists_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR NOT NULL, 
        location VARCHAR, 
        latitude NUMERIC, 
        longitude NUMERIC);
""")

times_table_create = ("""
    CREATE TABLE IF NOT EXISTS times (
        time_stamp BIGINT PRIMARY KEY, 
        hour INT NOT NULL, 
        day INT NOT NULL, 
        week INT NOT NULL, 
        month INT NOT NULL, 
        year INT NOT NULL, 
        weekday INT NOT NULL);
""")

# COPY tablename 
# FROM 'data_source' 
# CREDENTIALS 'credentials-args' 
# FORMAT AS { AVRO | JSON } 's3://jsonpaths_file';

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json {}
    STATUPDATE ON
    region 'us-west-2;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json
    STATUPDATE ON
    region 'us-west-2;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songs_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT s.user_id   AS song_id,
                        s.firstName AS title,
                        s.lastName  AS artist_id,
                        s.gender    AS year,
                        s.level     AS duration
        FROM staging_songs s
""")

artists_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT s.artist_id     AS artist_id,
                        s.artist_name   AS name,
                        s.location      AS location,
                        s.latitude      AS latitude,
                        s.longitude     AS longitude
        FROM staging_songs s
""")

users_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT e.user_id   AS user_id,
                        e.firstName AS first_name,
                        e.lastName  AS last_name,
                        e.gender    AS gender,
                        e.level     AS level
        FROM staging_events e
        WHERE e.page = 'NextSong'
""")

# SELECT TIMESTAMP 'epoch' + column_with_time_in_ms/1000 *INTERVAL '1 second'
# FROM ...

times_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT TIMESTAMP 'epoch' + 
            e.ts/1000 * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour from start_time)    AS hour,
            EXTRACT(day from start_time)     AS day,
            EXTRACT(week from start_time)    AS week,
            EXTRACT(month from start_time)   AS month,
            EXTRACT(year from start_time)    AS year,
            EXTRACT(weekday from start_time) AS weekday
        FROM staging_events e
        WHERE e.page = 'NextSong'        
""")

songplays_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT TIMESTAMP 'epoch' + 
            e.ts/1000 * INTERVAL '1 second'  AS start_time, 
            e.userId                         AS user_id,
            e.level                          AS level,
            s.song_id                        AS song_id,
            s.artist_id                      AS artist_id,
            e.sessionId                      AS session_id,
            e.location                       AS location,
            e.userAgent                      AS user_agent
        FROM staging_events e
        LEFT JOIN staging_songs s 
            ON  s.artist_name = e.artist 
            AND s.title.      = e.song 
        WHERE e.page = 'NextSong'    
""")


# QUERY LISTS

copy_table_queries   = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, 
                        artists_table_insert, times_table_insert]

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, 
                        users_table_create, songs_table_create, artists_table_create, times_table_create]

drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, 
                        users_table_drop, songs_table_drop, artists_table_drop, times_table_drop]

