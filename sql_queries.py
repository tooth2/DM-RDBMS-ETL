# DROP TABLE STATEMENTS
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLE STATEMENTS
songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays (songplay_id \
SERIAL CONSTRAINT songplay_pk PRIMARY KEY,  \
start_time timestamp NOT NULL REFERENCES time (start_time), \
user_id text NOT NULL REFERENCES users (user_id), level text, \
song_id text REFERENCES songs (song_id), artist_id text REFERENCES artists (artist_id),  \
session_id int NOT NULL, location text, user_agent text)"

user_table_create = "CREATE TABLE IF NOT EXISTS users (user_id text CONSTRAINT \
users_pk PRIMARY KEY, first_name text, last_name text, gender text, level text)"

song_table_create = "CREATE TABLE IF NOT EXISTS songs (song_id text CONSTRAINT \
songs_pk PRIMARY KEY, title text, artist_id text, year int, duration float)"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists (artist_id text CONSTRAINT artist_pk PRIMARY KEY, name text, location text, lattitude text, longitude text)"

time_table_create = "CREATE TABLE IF NOT EXISTS time (start_time timestamp CONSTRAINT time_pk PRIMARY KEY, \
hour int NOT NULL , day int NOT NULL , week int NOT NULL , month int NOT NULL , year int NOT NULL , weekday text)"

# Insert Records

songplay_table_insert = "INSERT INTO songplays (songplay_id, start_time, user_id, level, \
song_id, artist_id, session_id, location, user_agent) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)"

user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) \
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level "

song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration)  \
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING "

artist_table_insert = "INSERT INTO artists (artist_id, name, location, lattitude, longitude) \
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING"

time_table_insert = "INSERT INTO time (start_time, hour, day, week, month, year, weekday)  \
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING"


# FIND SONGS

song_select = "SELECT songs.song_id, artists.artist_id \
FROM songs JOIN artists ON songs.artist_id = artists.artist_id \
WHERE songs.title = %s AND artists.name=%s AND songs.duration=%s" 

# QUERY LISTS
create_table_queries = [user_table_create, artist_table_create, song_table_create, \
                         time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]