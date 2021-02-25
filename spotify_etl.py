import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3



""" 
    TRANSFORM/VALIDATION
"""


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")




def run_spotify_etl():
     
    database_location = "sqlite:///my_played_tracks.sqlite"
    user_id = "Mari Zeferino"
    token = "BQC1gzBUFdfRAODeXkfKDE2TEk5cRJZhfxUxEZgwmyt455Bc-gpl3Ykv7JPvecwRhGXwSjSWG3A3iRQkw7O5DoIqqFOepdywbdqMFPn88zO6SWs2qvgZkqLuI915-ik4ua_zrjQMkxAM-74j9AR0jWQD"


    """ 
    EXTRACT
    """


    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=token)
    } 

    # Convert time
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download the songs
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    #print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting the relevant data
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Prepare a dictionary to turn it into a dataframe
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
        }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)


    """ 
    TRANSFORM
    """


    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")


    """ 
    LOAD
    """


    engine = sqlalchemy.create_engine(database_location)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")

    