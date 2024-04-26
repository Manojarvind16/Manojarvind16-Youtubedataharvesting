from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, text, DateTime, ForeignKey
from sqlalchemy.exc import IntegrityError , DataError
import psycopg2
from psycopg2 import sql
import pandas as pd
import time
import re
from datetime import datetime,timedelta
import isodate
import streamlit as st


# Connection parameters
mydb_host = "localhost"
mydb_user = "postgres"
mydb_password = "###############"
mydb_port = "5432"
mydb_name = "my_youtube"#database connection

# Construct the connection string
connection_string = f"postgresql://{mydb_user}:{mydb_password}@{mydb_host}:{mydb_port}/{mydb_name}"


# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# SQL statement for creating the database if it doesn't exist
try:
    create_db_query = Text(f"CREATE DATABASE IF NOT EXISTS {mydb_name}")
    print("Database created successfully")

except:
    print ("Error while creating Database")

#channels table

mydb = psycopg2.connect(
                        host="localhost",
                        user="postgres",
                        password="###############",
                        port="5432",
                        database="my_youtube"
                    )

cursor=mydb.cursor()

try:
    create_query='''create table if not exists channels(Channel_Id varchar(80) primary key,
                                                        Channel_Name varchar(100),
                                                        Subscribers bigint,
                                                        Total_Videos int,
                                                        Views_count bigint,
                                                        Channel_Description text)'''
    cursor.execute(create_query)
    mydb.commit()
    print("successfully created")
except:
    print("unable to create table")

#videos table

try:
    create_query='''create table if not exists videos(Channel_Id varchar(80),
                                                        Channel_Name varchar(100),
                                                        Comments int,
                                                        Caption varchar(50),
                                                        Description text,
                                                        Definition varchar(10),
                                                        Duration interval,
                                                        Fav_Count int,
                                                        Published_Date Timestamp,
                                                        Thumbnail varchar(200),
                                                        Video_Id varchar(30) primary key,
                                                        Video_Title varchar(150),
                                                        Views bigint,
                                                        Likes bigint)'''
    cursor.execute(create_query)
    mydb.commit()

except Exception as e:
    print("An error occurred:", str(e))


#comments table

try:
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_id varchar(50),
                                                        Comment_TEXT text,
                                                        Comment_Author varchar(150),
                                                        Published_Date Timestamp)'''
    cursor.execute(create_query)
    mydb.commit()
    
except Exception as e:
    print("An error occurred:", str(e))


#to connect api
def Api_connect():
    Api_id="AIzaSyDlzhTRUJaYbEbjNmkTMQDgo-WyuHph6UI"
    Api_servicename="Youtube"
    Api_version="v3"

    Youtube=build(Api_servicename,Api_version,developerKey=Api_id)

    return Youtube

youtube=Api_connect()


#get channels information
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    if 'items' in response:
        for i in response['items']:
            data = dict(
                Channel_Id=i["id"],
                Channel_Name=i["snippet"]["title"],
                Subscribers=i['statistics']['subscriberCount'],
                Total_Videos=i["statistics"]["videoCount"],
                Views_count=i["statistics"]["viewCount"],               
                Channel_Description=i["snippet"]["description"]
            )
        return data

# get videos ids
def get_videos_ids(channel_id):
    video_ids = []
    try:
        response = youtube.channels().list(
            id=channel_id,
            part='contentDetails').execute()

        Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token = None

        while True:
            response1 = youtube.playlistItems().list(
                part='snippet',
                playlistId=Playlist_Id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            if 'items' in response1:
                for i in range(len(response1['items'])):
                    video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token = response1.get('nextPageToken')

                if next_page_token is None:
                    break
            else:
                print("No 'items' key in response1:", response1)
                break

    except Exception as e:
        print("An error occurred:", e)

    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        if 'items' in response:
            for item in response["items"]:
                data=dict(Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Comments=item['statistics'].get('commentCount'),
                        Caption=item['contentDetails']['caption'],
                        Description=item['snippet'].get('description'),
                        Definition=item['contentDetails']['definition'],
                        Duration=item['contentDetails']['duration'],
                        Fav_Count=item['statistics']['favoriteCount'],
                        Published_Date=item['snippet']['publishedAt'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Video_Id=item['id'],
                        Video_Title=item['snippet']['title'],                                   
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        )
                video_data.append(data)    
    return video_data

#get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            if 'items' in response:
                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_TEXT=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                    Comment_data.append(data)
                
    except:
        pass
    return Comment_data

# Convert 'Duration' column from ISO 8601 duration format to HH:MM:SS
def convert_duration(duration_str):
    parts = re.findall(r'(\d+)([A-Za-z]+)', duration_str)
    hours = 0
    minutes = 0
    seconds = 0
    for value, unit in parts:
        if unit.startswith('H'):
            hours += int(value)
        elif unit.startswith('M'):
            minutes += int(value)
        elif unit.startswith('S'):
            seconds += int(value)
    total_seconds = hours * 3600 + minutes * 60 + seconds
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Function to fetch data from YouTube API
def fetch_data(channel_id):
    # Call your existing functions to get data
    channel_details = get_channel_info(channel_id)
    video_ids = get_videos_ids(channel_id)
    video_details = get_video_info(video_ids)
    comment_details = get_comment_info(video_ids)

    # Convert data to dataframes
    channel_df = pd.DataFrame([channel_details])
    video_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)

    # Convert columns to appropriate data types
    #videos table

    video_df['Channel_Id'] = video_df['Channel_Id'].astype(str)
    video_df['Channel_Name'] = video_df['Channel_Name'].astype(str)
    video_df['Comments'] = video_df['Comments'].fillna(0).astype(int)
    video_df['Caption'] = video_df['Caption'].astype(str)
    video_df['Description'] = video_df['Description'].astype(str)
    video_df['Definition'] = video_df['Definition'].astype(str)
    video_df['Duration_HMS'] = video_df['Duration'].apply(convert_duration)
    video_df['Fav_Count'] = video_df['Fav_Count'].astype(int)
    video_df['Published_Date'] = pd.to_datetime(video_df['Published_Date'])  
    video_df['Thumbnail'] = video_df['Thumbnail'].astype(str)
    video_df['Video_Id'] = video_df['Video_Id'].astype(str)
    video_df['Video_Title'] = video_df['Video_Title'].astype(str)
    video_df['Views'] = video_df['Views'].astype(int)
    video_df['Likes'] = video_df['Likes'].fillna(0).astype(int)

    #comments table
    comment_df['Published_Date'] = pd.to_datetime(comment_df['Published_Date'])  

    return channel_df, video_df, comment_df
    
    try:
        # Store dataframes in the PostgreSQL database
        channel_df.to_sql('channels', engine, if_exists='append', index=False)
        video_df.to_sql('videos', engine, if_exists='append', index=False)
        comment_df.to_sql('comments', engine, if_exists='append', index=False)

        st.success("Data pushed successfully to PostgreSQL.")

    except Exception as e:
        st.error("An error occurred while pushing data to PostgreSQL:", str(e))

# Function to execute SQL queries and fetch results
def execute_query(query):
    # Connection parameters
    mydb_host = "localhost"
    mydb_user = "postgres"
    mydb_password = "###############"
    mydb_port = "5432"
    mydb_name = "my_youtube"

    # Construct the connection string
    connection_string = f"postgresql://{mydb_user}:{mydb_password}@{mydb_host}:{mydb_port}/{mydb_name}"

    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)

    # Create a text object from the query string
    query_text = sqlalchemy.text(query)

    # Execute the query and fetch results
    with engine.connect() as connection:
        result = connection.execute(query_text)
        data = result.fetchall()
        columns = result.keys()
    
    return pd.DataFrame(data, columns=columns)

# Main function to run Streamlit app
def main():
    with st.sidebar:
        st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
        st.header("Channel Information")
        st.caption("Data Management using Python and SQL")
        st.caption("Data Collection")
        st.caption("API Integration")

    # Input bar for channel ID
    channel_id = st.text_input("Enter Channel ID:")

    if st.button("Fetch Data"):
        # Fetch data from YouTube API
        channel_df, video_df, comment_df = fetch_data(channel_id)

        # Display dataframes
        st.subheader("Channel Data")
        st.write(channel_df)

        st.subheader("Video Data")
        st.write(video_df)

        st.subheader("Comment Data")
        st.write(comment_df)

    if st.button("Push to Database"):
        # Store data in PostgreSQL database
        store_data_to_postgres(channel_df, video_df, comment_df)

    selected_query = st.selectbox("Select a question", [
                                                    "1. What are the names of all the videos and their corresponding channels?",
                                                    "2. Which channels have the most number of videos, and how many videos do they have?",
                                                    "3. What are the top 10 most viewed videos and their respective channels?",
                                                    "4. How many comments were made on each video, and what are their corresponding video names?",
                                                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8. What are the names of all the channels that have published videos in the year 2022?",
                                                    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                                                ])

    if st.button("Execute"):
        query_number = int(selected_query.split('.')[0])
        if query_number == 1:
            query = 'SELECT "Video_Title", "Channel_Name" FROM videos;'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 2:
            query = 'SELECT "Channel_Name", COUNT(*) as "Video_Count" FROM videos GROUP BY "Channel_Name" ORDER BY "Video_Count" DESC;'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 3:
            query = 'SELECT "Video_Title", "Channel_Name", "Views" FROM videos ORDER BY "Views" DESC LIMIT 10;'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 4:
            query = 'SELECT "Video_id", COUNT(*) as "Comment_Count" FROM comments GROUP BY "Video_id";'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 5:
            query = 'SELECT "Video_Title", "Channel_Name", "Likes" FROM videos ORDER BY "Likes" DESC;'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 6:
            query = 'SELECT "Video_Id", SUM("Likes") as "Total_Likes" FROM videos GROUP BY "Video_Id";'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 7:
            query = 'SELECT "Channel_Name", SUM("Views") as "Total_Views" FROM videos GROUP BY "Channel_Name";'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 8:
            query = 'SELECT DISTINCT "Channel_Name" FROM videos WHERE EXTRACT(YEAR FROM "Published_Date") = 2022;'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 9:
            query = 'SELECT "Channel_Id", AVG(EXTRACT(EPOCH FROM "Duration_HMS"::interval)) AS "Average_Duration" FROM videos GROUP BY "Channel_Id";'
            result_df = execute_query(query)
            st.write(result_df)

        elif query_number == 10:
            query = 'SELECT "Video_id", COUNT(*) as "Comment_Count" FROM comments GROUP BY "Video_id" ORDER BY "Comment_Count" DESC LIMIT 1;'
            result_df = execute_query(query)
            st.write(result_df)

if __name__ == "__main__":
    main()
