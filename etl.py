import os
import glob
import psycopg2
import pandas as pd
from logging import error, info
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Read json format song records file from an argument filepath
    - Extract song data and insert to the song table
    - Extract artist data d and insert to the artist table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record  
    artist_data = artist_data = df[['artist_id', 'artist_name', 'artist_location',  \
                                    'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    - Read json format song records file from an argument filepath
    - Convert timestamp to dateTime and insert time data into time table 
    - Extract user data and insert to the user table
    - Insert songplay data to the songply tables by getting songid and artistid 
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime 
    t = df.copy()
    t['ts'] = pd.to_datetime(t['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.ts, t.ts.dt.hour , t.ts.dt.day , t.ts.dt.dayofweek , t.ts.dt.month , \
                 t.ts.dt.year , t.ts.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_dict = {"start_time":t.ts , 
             "hour":t.ts.dt.hour,
             "day":t.ts.dt.day,
             "week":t.ts.dt.dayofweek,
             "month":t.ts.dt.month,
             "year":t.ts.dt.year,
             "weekday":t.ts.dt.weekday
            }
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user data
    user_df = df[['userId', 'firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        starttime = pd.to_datetime(row.ts,unit='ms')
        

        # insert songplay record
        songplay_data = (starttime, row.userId, row.level, songid, artistid, \
                         row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Read all json files from directory given by an argument file path 
    - stores file names  
    - print number of files processed
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
    """
    - Connect to Postgres databse and gets a connection and coursor instance to process
    - Call process_data function to load songs and log data
    - After finishing two process_data call , close the database connection
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        info("Successfully connected to Sparkify DB and retrived associated connection and cursor")
    except Exception as e:
        error(f"Error connecting to Sparkify DB: {e}")
        exit()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()