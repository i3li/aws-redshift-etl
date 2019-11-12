import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    id INT IDENTITY(0,1) PRIMARY KEY,
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
)

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL sortkey,
    artist_id VARCHAR NOT NULL,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY(songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT sortkey,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR sortkey,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR sortkey,
    name VARCHAR,
    location VARCHAR,
    longitude FLOAT,
    latitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP sortkey,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
)
""")

# STAGING TABLES

ARN = config.get("IAM_ROLE","ARN")

staging_events_copy = ("""
COPY staging_events FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}'
TIMEFORMAT 'epochmillisecs'
JSON 's3://udacity-dend/log_json_path.json' REGION 'us-west-2'
""").format(ARN)

staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' REGION 'us-west-2'
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) (SELECT ts, userId, level, song_id, artist_id, sessionId, location, userAgent FROM staging_events JOIN staging_songs ON song = title AND artist = artist_name AND duration = length WHERE userId IS NOT NULL)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) (SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE userId IS NOT NULL)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) (SELECT song_id, title, artist_id, year, duration FROM staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) (SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs)
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) (
    SELECT 
    ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(weekday FROM ts)
    FROM staging_events
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
