import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import logging
import time


def process_song_file(cur, filepath):
     """
    - Load data from a song file to the song and artist data tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location',
                       'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
     """
    - Load data from a log file to the time, user and songplay data tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = [(tt.value, tt.hour, tt.day, tt.week, tt.month, tt.year, tt.weekday()) for tt in t]
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

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
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'],
                         row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Iterate over all files and populate data tables in sparkifydb
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

    start_time = time.time()

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"Processed {num_files} files in {processing_time:.2f} seconds")

def check_data_quality(df, table_name):
    """
    Perform data quality checks on the dataframe before insertion
    """
    print(f"Performing data quality checks for {table_name}")
    
    total_rows = len(df)
    null_counts = df.isnull().sum()
    
    print(f"Total rows: {total_rows}")
    print("Null value counts:")
    print(null_counts)
    
    if table_name == 'songs':
        # Check for negative years or durations
        invalid_years = df[df['year'] < 0].shape[0]
        invalid_durations = df[df['duration'] <= 0].shape[0]
        print(f"Songs with invalid years: {invalid_years}")
        print(f"Songs with invalid durations: {invalid_durations}")
    
    # Add more specific checks for other tables as needed

# Call this function before inserting data in process_song_file and process_log_file

# Set up logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')
def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.
    
    - Runs ETL pipelines
    """
    try :
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()

        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)

        conn.close()
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()