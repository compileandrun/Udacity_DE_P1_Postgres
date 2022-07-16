# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS  users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS date_time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE songplays (
							songplay_id serial PRIMARY KEY, 
							start_time timestamp NOT NULL, 
							user_id int references users (user_id), 
							level varchar, 
							song_id varchar, -- I wanted to put NOT NULL constraint here. But if this constraint is violated, 
							-- I get an error. And postgres do not have any function to get around this.
							-- SO either as here I have to get rid of this constraint or I have to clean the data and try again.
							--https://stackoverflow.com/questions/59430790/null-value-in-column-violates-not-null-constraint-postgresql 
							artist_id varchar references artists (artist_id), 
							session_id varchar, 
							location varchar, 
							user_agent varchar) """)

user_table_create = (""" CREATE TABLE users (
						user_id int PRIMARY KEY, 
						first_name varchar,
						last_name varchar, 
						gender varchar, 
						level varchar NOT NULL) """)

song_table_create = (""" CREATE TABLE songs (
						song_id varchar PRIMARY KEY, 
						title varchar NOT NULL, 
						artist_id varchar NOT NULL, 
						year int NOT NULL, 
						duration double precision NOT NULL) """)

artist_table_create = (""" CREATE TABLE artists (
							artist_id varchar PRIMARY KEY, 
							artist_name varchar NOT NULL, 
							location varchar, 
							latitude double precision, 
							longitude double precision) """)

time_table_create = (""" CREATE TABLE date_time (
						start_time timestamp PRIMARY KEY, 
						hour int, 
						day int, 
						week int, 
						month int, 
						year int, 
						weekday int)  """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, 
							song_id, artist_id, session_id, location, user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
							ON CONFLICT (songplay_ID)
							DO NOTHING""")

user_table_insert = ("""INSERT INTO users (user_id, first_name,last_name, gender, level) VALUES (%s,%s,%s,%s,%s)
						ON CONFLICT (user_ID)
						DO UPDATE SET level = EXCLUDED.level""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s,%s,%s,%s,%s)
						ON CONFLICT DO NOTHING
						""")

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, location, latitude, longitude) VALUES (%s,%s,%s,%s,%s)
						ON CONFLICT (artist_id)
						DO UPDATE SET 
							artist_name = EXCLUDED.artist_name,
							location = EXCLUDED.location,
							latitude = EXCLUDED.latitude,
							longitude = EXCLUDED.longitude""")


time_table_insert = ("""INSERT INTO date_time (start_time, hour, day, week, month, year, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)
						ON CONFLICT DO NOTHING""")

# FIND SONGS

song_select = ("""
	SELECT 
	s.song_id,
	a.artist_id
	FROM artists a
INNER JOIN songs s
ON a.artist_id = s.artist_id
WHERE s.title = %s
AND a.artist_name = %s
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]