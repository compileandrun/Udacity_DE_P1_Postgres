import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description:
        This function reads the song data and inserts the selected parts of the data it into
        the songs & artists tables.

    Arguments:
        cur: cursor object
        filepath: path to the song data file

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath,typ="series")

    # insert song record
    song_data = [df.song_id,df.title,df.artist_id,df.year,df.duration]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.artist_id,df.artist_name,df.artist_location,df.artist_latitude,df.artist_longitude]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
     Description:   
        This function reads the log data & cleans it by doing some filtering and resetting datatypes. 
        It then inserts the start_time column into date_time table with added some transformations.
        It inserts data to users table.
        Finally, it runs a sql query to create the meaningful songplay data and inserts it into the song_plays table.

    Arguments:
        cur: cursor object
        filepath: path to the log data file

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page=="NextSong"].reset_index(drop=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.isocalendar().week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('start_time', "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.concat(time_data,axis=1,keys=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        
        if results:
            print(results)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (t[index],row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description:
        This function reads all the files in the given folder by iterating on each file 1-by-1. 
        Then, it inserts the data in each file one by one to the relevant table.

    Arguments:
        cur: cursor object
        conn: object for the connection to the database
        filepath: path to the song or long data file
        func: function to transform & insert data to a database table

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
    print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=koray_v2")
    cur = conn.cursor()

    process_data(cur, conn, filepath='/Users/koray/Documents/Udacity_DE/project-template/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='/Users/koray/Documents/Udacity_DE/project-template/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()