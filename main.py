from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, VARCHAR, Time, ForeignKey, text
from sqlalchemy.exc import IntegrityError
import mysql.connector
import pandas as pd
import time
import datetime
import isodate
import streamlit as st

#to connect api
def Api_connect():
    Api_id="AIzaSyDxblM4kFsTUFb2rj3_wxN2M2taNRjILVU"
    Api_servicename="Youtube"
    Api_version="v3"

    Youtube=build(Api_servicename,Api_version,developerKey=Api_id)

    return Youtube

youtube=Api_connect()

# Database connection
mydb_host = "127.0.0.1"
mydb_port = "3306"
mydb_user = "root"
mydb_password = "Kiprthmass2170."
mydb_name = "youtube_data"

# Connection URL with specifying the database
connection_url = f"mysql+mysqlconnector://{mydb_user}:{mydb_password}@{mydb_host}/{mydb_name}"

# Create engine with the connection URL
engine = create_engine(connection_url)

# Connect to MySQL server
try:
    connection = mysql.connector.connect(
        host=mydb_host,
        port=mydb_port,
        user=mydb_user,
        password=mydb_password
    )

    cursor = connection.cursor()

    # Create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mydb_name}")

    print("Database created successfully!")
    
except mysql.connector.Error as err:
    print(f"An error occurred: {err}")

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

# Define metadata
metadata = MetaData()

# Define channels table
channels_table = Table(
    'channels',
    metadata,
    Column('Channel_id', String(255), primary_key=True),
    Column('Channel_Name', String(255)),
    Column('Subscribers', Integer),
    Column('Total_videos', Integer),
    Column('Views_count', Integer),
    Column('Channel_Description', String(255)),
    Column('Playlist_id', String(255)),
    schema=mydb_name
)

# Define videos table
videos_table = Table(
    'videos',
    metadata,
    Column('Channel_Id', String(255), ForeignKey(f'{mydb_name}.channels.Channel_id')),
    Column('Channel_Name', String(255)),
    Column('Comments', Integer),
    Column('Caption', String(10)),
    Column('Description', String(500)),
    Column('Definition', String(10)),
    Column('Duration', String(50)),
    Column('Fav_Count', Integer),
    Column('Published_date', DateTime),
    Column('Thumbnail', String(500)),
    Column('Video_Id', String(255), primary_key=True),
    Column('Video_Title', String(255)),
    Column('Views', Integer),
    Column('Likes', Integer),
    Column('Dislikes', Integer),
    schema=mydb_name
)

# Define comments table
comments_table = Table(
    'comments',
    metadata,
    Column('Comment_Id', String(255), primary_key=True),
    Column('Video_id', String(255), ForeignKey(f'{mydb_name}.videos.Video_Id')),
    Column('Comment_TEXT', String(500)),
    Column('Comment_Authour', String(255)),
    Column('Published_Date', DateTime),
    schema=mydb_name
)

# Create tables
metadata.create_all(engine)

print("MySQL tables created successfully.")

#to get channel info

def channel_info(channel_id):
    request = youtube.search().list(
        part="snippet",
        q=channel_id,
    )
    response = request.execute()

    if not response['items']:
        print("No channel found with the given name.")
        return None

    channel_id = response['items'][0]['snippet']['channelId']

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for item in response['items']:
        ch_data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_id': item['id'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Total_videos': item['statistics']['videoCount'],
            'Views_count': item['statistics']['viewCount'],
            'Channel_Description': item['snippet']['description'],
            'Playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
        }
    return ch_data

#to get video ids

def video_ids(channel_id):
    channel_data = channel_info(channel_id)
    if channel_data is None:
        return None

    ch_id = channel_data['Channel_id']

    video_ids = []
    response = youtube.channels().list(id=ch_id, part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response_vd = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response_vd['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response_vd.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#to get video info

def video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        retries = 3  # Number of retries
        for _ in range(retries):
            try:
                request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=video_id
                )
                response = request.execute()

                for item in response['items']:
                    Video_Duration_ISO = item['contentDetails']['duration']
                    # Convert ISO 8601 duration to time format (HH:MM:SS)
                    duration_seconds = isodate.parse_duration(Video_Duration_ISO).total_seconds()
                    Video_Duration = str(datetime.timedelta(seconds=duration_seconds))

                    vd_data = {
                        'Channel_Id': item['snippet']['channelId'],
                        'Channel_Name': item['snippet']['channelTitle'],
                        'Comments': item.get('commentCount'),
                        'Caption': item['contentDetails']['caption'],
                        'Description': item.get('description'),
                        'Definition': item['contentDetails']['definition'],
                        'Duration': Video_Duration,
                        'Fav_Count': item['statistics'].get('favoriteCount'),
                        'Published_date': item['snippet']['publishedAt'],
                        'Likes': item['statistics'].get('likeCount'),
                        'Thumbnail': item['snippet']['thumbnails']['medium']['url'],  # Access medium-sized thumbnail URL Or Your Choice
                        'Video_Id': item['id'],
                        'Video_Title': item['snippet']['title'],
                        'Views': item['statistics'].get('viewCount')
                    }
                    video_data.append(vd_data)
                break  # Break out of the retry loop if successful
            except HttpError as e:
                if e.resp.status == 500:
                    print("Encountered 500 error. Retrying...")
                    time.sleep(1)  # Add a delay before retrying
                    continue  # Retry the request
                else:
                    raise  # Re-raise the exception if it's not a 500 error
    return video_data

# to get comment details 

def comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                C_data = {
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_Text': item['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                    'Comment_Authour': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Published_Date': item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                Comment_data.append(C_data)
    except Exception as e:
        print("An error occurred:", str(e))
    return Comment_data

st.write("Enter the YouTube channel id:")

# Get user input for channel name
channel_id = st.text_input("")

# Get channel information
channel_data = channel_info(channel_id)

# Get video IDs
video_ids_data = video_ids(channel_id)

# Get video details
video_details_data = video_info(video_ids_data)

# Get comment details
comment_details_data = comment_info(video_ids_data)


channel_df = pd.DataFrame([channel_data], columns=channel_data.keys())
video_details_df = pd.DataFrame(video_details_data, columns=video_details_data[0].keys())
comment_details_df = pd.DataFrame(comment_details_data, columns=comment_details_data[0].keys())

if st.button("Push Data to Database"):
     
    try:
         channel_df.to_sql('channel_info', con=engine, schema=db_name, if_exists='append', index=False)
         video_details_df.to_sql('video_info', con=engine, schema=db_name, if_exists='append', index=False)
         comment_details_df.to_sql('comment_info', con=engine, schema=db_name, if_exists='append', index=False) 
    

         print("Data pushed successfully to MySQL.")

    except Exception as e:

         print("An error occurred while pushing data to MySQL:", str(e))
         
st.write("Channel Info:")
st.write(channel_df)

st.write("video_info:")
st.write(video_details_df)

st.write("comment_info:")
st.write(comment_details_df)


         

# Streamlit Part
# Database connection
mydb_host = "127.0.0.1"
mydb_port = "3306"
mydb_user = "root"
mydb_password = "Kiprthmass2170."
mydb_name = "youtube_data"

# Connection URL with specifying the database
connection_url = f"mysql+mysqlconnector://{mydb_user}:{mydb_password}@{mydb_host}/{mydb_name}"

# Create engine with the connection URL
engine = create_engine(connection_url)

# Function to execute SQL queries
def execute_query(query):
    connection = engine.connect()
    result = connection.execute(query).fetchall()
    connection.close()
    return result


# Define Streamlit app

st.sidebar.title(":YouTube Data Harvesting And Warehousing")
st.sidebar.subheader("Channel Information")
st.sidebar.caption("All Data Management using Python and MySQL")
st.sidebar.caption("Data Collection From YouTube")
st.sidebar.caption("API Integration")
st.sidebar.markdown("---")

# Select query

selected_query = st.sidebar.selectbox("Select a question", [
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


if selected_query:
    if selected_query.startswith("1."):
        query = text("SELECT Video_Title, Channel_Name FROM videos")
        column_names = ["Video Title", "Channel Name"]

    elif selected_query.startswith("2."):
        query = text("SELECT Channel_Name, COUNT(*) AS num_videos FROM videos GROUP BY Channel_Name ORDER BY num_videos DESC")
        column_names = ["Channel Name", "Number of Videos"]

    elif selected_query.startswith("3."):
        query = text("SELECT Video_Title, Views, Channel_Name FROM videos ORDER BY Views DESC LIMIT 10")
        column_names = ["Video Title", "Views", "Channel Name"]

    elif selected_query.startswith("4."):
        query = text("SELECT v.Video_Title, COUNT(*) AS num_comments FROM comments c JOIN video_info v ON c.Video_id = v.Video_id GROUP BY v.Video_Title;")
        column_names = ["Video Title", "Number of Comments"]

    elif selected_query.startswith("5."):
        query = text("SELECT Video_Title, Likes, Channel_Name FROM videos ORDER BY Likes DESC LIMIT 10")
        column_names = ["Video Title", "Likes", "Channel Name"]

    elif selected_query.startswith("6."):
        query = text("SELECT Video_Title, SUM(Likes) AS total_likes, SUM(Dislikes) AS total_dislikes FROM videos GROUP BY Video_Title")
        column_names = ["Video Title", "Total Likes", "Total Dislikes"]

    elif selected_query.startswith("7."):
        query = text("SELECT Channel_Name, SUM(Views) AS total_views FROM videos GROUP BY Channel_Name")
        column_names = ["Channel Name", "Total Views"]

    elif selected_query.startswith("8."):
        query = text("SELECT DISTINCT Channel_Name FROM videos WHERE YEAR(Published_date) = 2022")
        column_names = ["Channel Name"]

    elif selected_query.startswith("9."):
        query = text("SELECT b.Channel_Name AS Channel_Name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(a.Duration))), '%H:%i:%s') AS AVERAGE_VIDEO_DURATION FROM videos a JOIN channel_info b ON a.Channel_Id = b.Channel_id GROUP BY Channel_Name ORDER BY AVERAGE_VIDEO_DURATION DESC;")
        column_names = ["Channel Name", "Average Duration"]

    elif selected_query.startswith("10."):
        query = text("SELECT Video_Title, Comments, Channel_Name FROM videos ORDER BY Comments DESC LIMIT 10")
        column_names = ["Video Title", "Comments", "Channel Name"]

    # Execute the query
    result_data = execute_query(query)
    
    # Display the results
    if result_data:
        df = pd.DataFrame(result_data, columns=column_names)
        st.dataframe(df)
        
    else:
        st.warning("No data available for the selected query.")