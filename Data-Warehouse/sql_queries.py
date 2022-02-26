import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get('S3', 'LOG_DATA')
SONG_DATA=config.get('S3', 'SONG_DATA')
IAM_ROLE=config.get('IAM_ROLE','ARN')
LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
location varchar,
name varchar, 
song_id varchar, 
user_id int NOT NULL, 
first_name varchar, 
last_name varchar,
gender char,
level varchar,
auth varchar,
items_in_session int,
length float,
method varchar,
page varchar,
registration varchar,
session_id int,
status int,
start_time TIMESTAMP NOT NULL,
user_agent varchar,
PRIMARY KEY(user_id)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs int,
artist_id varchar,
latitude numeric, 
longitude numeric,
location varchar,
name varchar, 
song_id varchar, 
title varchar, 
year int,
duration float,
PRIMARY KEY(song_id)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id int,
user_id int, 
start_time TIMESTAMP,
level varchar,
song_id varchar, 
artist_id varchar,
session_id int, 
location varchar, 
user_agent varchar,
PRIMARY KEY(song_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id int, 
first_name varchar, 
last_name varchar,
gender char, 
level varchar,
PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id varchar, 
title varchar, 
artist_id varchar, 
year int,
duration float,
PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
artist_id varchar, 
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric,
PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
start_time TIMESTAMP NOT NULL,
hour int, 
day int,
weekofyear int,
weekday int,
month int, 
year int,
PRIMARY KEY(start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(LOG_DATA,IAM_ROLE)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time,user_id, level, song_id, artist_id, session_id,location, user_agent)
SELECT DISTINCT staging_events.ts,
staging_events.userid, staging_events.level, staging_songs.song_id,
staging_songs.artist_id, staging_events.sessionid, staging_songs.artist_location,
staging_events.useragent
FROM staging_songs
JOIN staging_events
ON staging_songs.title = staging_events.song
""")

user_table_insert = ("""
INSERT INTO users 
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
""")

song_table_insert = ("""
INSERT INTO songs 
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists 
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, name, location,latitude, longitude
FROM staging_songs
""")


time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, weekofyear, weekday, month, year)
SELECT DISTINCT ts, extract(hour from ts), extract(day from ts),extract(week from ts), extract(month from ts),extract(year from ts), 
extract(weekday from ts)
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
