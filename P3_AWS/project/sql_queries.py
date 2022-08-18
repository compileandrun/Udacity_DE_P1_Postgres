import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('project_dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS playtimes CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
	CREATE TABLE staging_events (
	artist 	varchar, 
    auth   	varchar, 
    firstName 	varchar, 
    gender 	varchar, 
    itemInSession 	int,  
    lastName  		varchar, 
    length 	float8,
    level  	varchar, 
    location  	varchar, 
    method 	varchar, 
   page   	varchar ,
   registration 	bigint,  
   sessionId 	int,  
   song   	varchar, 
   status 	int,  
   ts     	bigint,  
   userAgent 	varchar, 
   userId 	int  
 )

""")

staging_songs_table_create = ("""
	CREATE TABLE staging_songs (
	artist_id varchar,
 	artist_latitude   float8,
    artist_location   varchar,
    artist_longitude  float8,
    artist_name       varchar, 
    duration          float8,
    num_songs         int,  
    song_id           varchar, 
    title             varchar,
    year 			  int  )
""")

songplay_table_create = ("""
	CREATE TABLE songplay (
	songplay_id int IDENTITY(1,1) sortkey,
	start_time bigint , 
	user_id int , 
	level varchar, 
	song_id varchar distkey, 
	artist_id varchar, 
	session_id int, 
	location varchar, 
	user_agent varchar

	)
""")

user_table_create = ("""
	CREATE TABLE users (
	user_id int sortkey, 
	first_name varchar, 
	last_name varchar, 
	gender varchar, 
	level varchar
	)
""")

song_table_create = ("""
	CREATE TABLE songs (
	song_id varchar sortkey distkey, 
	title varchar, 
	artist_id varchar, 
	year INT, 
	duration float8
	)
""")

artist_table_create = ("""
	CREATE TABLE artists (
	artist_id varchar  sortkey,
	name varchar, 
	location varchar, 
	latitude float8, 
	longitude float8
	)
""")

time_table_create = ("""
	CREATE TABLE playtimes (
	start_time bigint sortkey, 
	hour INT, 
	day int, 
	week int, 
	month int, 
	year int, 
	weekday int
	)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/2018/11'
    credentials 'aws_iam_role={}'
    format json as 's3://udacity-dend/log_json_path.json'     
    dateformat 'auto';
""").format(DWH_ROLE_ARN)
#fromat line  helps with formatting of column names and thus match the ones in S3 and staging_events table.                       

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/A/A'
    credentials 'aws_iam_role={}'
    format as json 'auto' 
    region 'us-west-2';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
	INSERT INTO songplay (start_time, user_id,level,song_id,artist_id,session_id,location,user_agent)
	SELECT 
		e.ts,
	    e.userid,
	    e.level,
	    s.song_id,
	    s.artist_id,
	    e.sessionid,
	    e.location,
	    e.useragent
	FROM staging_events e
	inner JOIN staging_songs s
	ON e.song = s.title
""")

user_table_insert = ("""
	INSERT INTO users (user_id,first_name,last_name,gender,level)
	SELECT distinct 
		userId,
		firstname,
		lastname,
		gender,
		level
	FROM staging_events
""")

song_table_insert = ("""
	INSERT INTO songs (song_id,title,artist_id,year,duration)
	SELECT distinct 
		song_id,
		title,
		artist_id,
		year,
		duration
	FROM staging_songs
""")

artist_table_insert = ("""
	INSERT INTO artists (artist_id,name,location,latitude,longitude)
	SELECT distinct 
		artist_id,
		artist_name,
		artist_location,
		artist_latitude,
		artist_longitude
	FROM staging_songs
""")

time_table_insert = ("""
	INSERT INTO playtimes (start_time,hour,day,week,month,year,weekday)
	SELECT start_time,
		EXTRACT(HOUR from start_date) as hour,
		EXTRACT(DAY from start_date) as day,
		EXTRACT(WEEK from start_date) as week,
		EXTRACT(MONTH from start_date) as month,
		EXTRACT(YEAR from start_date) as year,
		EXTRACT(WEEKDAY from start_date) as weekday
	FROM (SELECT ts as start_time, timestamp 'epoch' + cast(ts AS bigint)/1000 * interval '1 second' AS start_date from staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]