# For the ACTIVATION of resource we need Two parameters ( youtube , channel_ID ) and googlapiclient python library :pip install google-api-client

from googleapiclient.discovery import build

# import mongoDB 
import pymongo

# import postgreSQL
import psycopg2


# import pandas 
import pandas as pd

# import streamlit 
import streamlit as st

# the "build" connects youtube data and api key as one parameter 
API_key = "AIzaSyCoWj9wBP1mSQg3mVZAWHp36yA56ElnSfg"

#youtube = build(api service name, version, developerKey=api key)
youtube = build('youtube','v3',developerKey = API_key)
   


# get channel details : snippet, statistics,content details 

request = youtube.channels().list(
     id = "UCxqAWLTk1CmBvZFPzeZMd9A",
     part =  'snippet,statistics,contentDetails'

)
channel_data = request.execute()


# get the channel details 

def get_channel_details(channel_id):
    response = youtube.channels().list(
                        id = channel_id,
                        part =  'snippet,statistics,contentDetails'

    )
    channel_data = response.execute()

    for i in channel_data['items']:
            channel_informations = dict(channel_name = i['snippet']['title'],
                    channel_id = i['id'],
                    channel_description= i['snippet']['description'],
                    playlist_ID= i['contentDetails']['relatedPlaylists']['uploads'],
                    subscribers= i['statistics']['subscriberCount'],
                    views= i['statistics']['viewCount'],
                    total_videos= i['statistics']['videoCount'])
                    
    return channel_informations

# to get video ids for multiple channel ids 

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id = channel_id,                                                                                                                                                                                                               
                                    part =  'contentDetails').execute()
    Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
            response1 = youtube.playlistItems().list( 
                part = 'snippet',
                playlistId=Playlist_ID,
                maxResults=50,
                pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = response1.get('nextPageToken')

            if next_page_token is None:
                break
    return video_ids

# get video information 

def get_video_details(Video_IDs):
    video_data = []
    for ids in Video_IDs:
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = ids
        )
        response = request.execute()
        for item in response['items']:
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        title = item['snippet']['title'],
                        tags = item['snippet'].get('tags'),
                        thumbnail =item['snippet']['thumbnails']['default']['url'],
                        video_description = item['snippet'].get('description'),
                        published_date = item['snippet']['publishedAt'],
                        duration = item['contentDetails']['duration'],
                        views = item['statistics'].get('viewCount'),
                        likes = item['statistics'].get('likeCount'),
                        dislikes = item['statistics'].get('dislikeCount'),
                        comments = item['statistics'].get('commentCount'),
                        favourite_count = item['statistics']['favoriteCount'],
                        caption_status = item['contentDetails']['caption'])
            video_data.append(data)
    return video_data


# to get comment details 

def get_comment_details(video_ids):
    comment_data = []
    try:
        for video_Id in video_ids:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId = video_Id, 
                maxResults = 50
                )
            response = request.execute()
            for item in response['items']:
                data = dict(comment_id = item['snippet']['topLevelComment']['id'],
                            video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']) 

                comment_data.append(data)

    except:
        pass
    return comment_data

#get playlist details 

def get_playlist_details(channel_id):
    next_page_token = None
    playlist_data = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token 
            )
        response = request.execute()

        for item in response['items']:
            data = dict(
                playlist_id = item['id'],
                channel_id = item['snippet']['channelId'],
                title = item['snippet']['title'],
                channel_name = item['snippet']['channelTitle'],
                published_At = item['snippet']['publishedAt'],
                video_count = item['contentDetails']['itemCount']
                )
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data

# connection to MongoDB

client = pymongo.MongoClient("mongodb+srv://Santhi:santhikichu@cluster0.jwz3noh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['YTB_DATA']

def Channel_Details(channel_id):
      Ch_details = get_channel_details(channel_id)
      Vi_ids = get_video_ids(channel_id)
      Ply_details = get_playlist_details(channel_id)
      Vi_details = get_video_details(Vi_ids)
      Com_details = get_comment_details(Vi_ids)
      
      
      coll1 = db["Channel_Details"]
      coll1.insert_one({'Channel_Informations':Ch_details,
                     'Playlist_Informations':Ply_details,
                     'Video_Informations':Vi_details,
                     'Comment_Informations':Com_details})
      return "Uploaded Successfully"

#table for channel details

def Channels_Table(channel_names):
        mydb = psycopg2.connect(host='localhost',
                                user= 'postgres',
                                password = 'santhi@1998',
                                database = 'youtube_data',
                                port='5432')
        cursor = mydb.cursor()

        create_query = '''create table if not exists channels(channel_name varchar(100),
                                        channel_id  varchar(80) primary key,
                                        channel_description text,  
                                        playlist_ID varchar(80),
                                        subscribers bigint,
                                        views bigint,
                                        total_videos int)'''
        cursor.execute(create_query)
        mydb.commit()
        
        # unique channel details 
        individual_channel_details = []
        db = client["YTB_DATA"]
        coll1 = db["Channel_Details"]
        for ch_data in coll1.find({"Channel_Informations.channel_name":channel_names},{"_id":0}):
                individual_channel_details.append(ch_data['Channel_Informations'])
        df_unique_ch = pd.DataFrame(individual_channel_details)


        for index,row in df_unique_ch.iterrows():
                insert_query = '''insert into channels(channel_name,
                                                        channel_id,
                                                        channel_description,
                                                        playlist_ID,
                                                        subscribers,
                                                        views,
                                                        total_videos) 
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                values = (row['channel_name'],
                        row['channel_id'],
                        row['channel_description'],
                        row['playlist_ID'],
                        row['subscribers'],
                        row['views'],
                        row['total_videos'])
                
                try:
                        cursor.execute(insert_query,values)
                        mydb.commit()

                except:
                        result = f"{channel_names} is already inserted"
                        return result
                        

# get playlist table 

def Playlist_Table(channel_names):
        mydb = psycopg2.connect(host='localhost',
                                        user= 'postgres',
                                        password = 'santhi@1998',
                                        database = 'youtube_data',
                                        port='5432')
        cursor = mydb.cursor()

        create_query = '''create table if not exists playlists(playlist_id varchar(200) primary key,
                                                        title varchar(200),
                                                        channel_id varchar(80) ,
                                                        channel_name text,  
                                                        published_At timestamp,
                                                        video_count int)'''
        cursor.execute(create_query)
        mydb.commit()

        
        individual_playlist_details = []
        db = client["YTB_DATA"]
        coll1 = db["Channel_Details"]
        for pl_data in coll1.find({"Channel_Informations.channel_name":channel_names},{"_id":0}):
                        individual_playlist_details.append(pl_data['Playlist_Informations'])
        df_unique_pl = pd.DataFrame(individual_playlist_details[0])
        
        for index,row in df_unique_pl.iterrows():
                        insert_query = '''insert into playlists(playlist_id,
                                                title,
                                                channel_id,
                                                channel_name,
                                                published_At,
                                                video_count) 
                                                                
                                                values(%s,%s,%s,%s,%s,%s)'''

                        values = (row['playlist_id'],
                                row['title'],
                                row['channel_id'],
                                row['channel_name'],
                                row['published_At'],
                                row['video_count'])

                        cursor.execute(insert_query,values)
                        mydb.commit()



# video table 

def Videos_Table(channel_names):

        mydb = psycopg2.connect(host='localhost',
                                user= 'postgres',
                                password = 'santhi@1998',
                                database = 'youtube_data',
                                port='5432')
        cursor = mydb.cursor()

        create_query = '''create table if not exists videos(channel_name varchar(100),
                                channel_id varchar(80),
                                video_id varchar(30) primary key,
                                title varchar(100),
                                tags text,
                                thumbnail varchar(200),
                                video_description text,
                                published_date timestamp,
                                duration interval,
                                views bigint,
                                likes bigint,
                                dislikes bigint,
                                comments int,
                                favourite_count int,
                                caption_status varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()

        individual_video_details = []
        db = client["YTB_DATA"]
        coll1 = db["Channel_Details"]
        for vi_data in coll1.find({"Channel_Informations.channel_name":channel_names},{"_id":0}):
                        individual_video_details.append(vi_data['Video_Informations'])
        df_unique_vi = pd.DataFrame(individual_video_details[0])

        for index,row in df_unique_vi.iterrows():
                insert_query = '''insert into videos(channel_name,
                                channel_id,
                                video_id,
                                title,
                                tags,
                                thumbnail,
                                video_description,
                                published_date,
                                duration,
                                views,
                                likes,
                                dislikes,
                                comments,
                                favourite_count,
                                caption_status) 
                                                        
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

                values = (row['channel_name'],
                        row['channel_id'],
                        row['video_id'],
                        row['title'],
                        row['tags'],
                        row['thumbnail'],
                        row['video_description'],
                        row['published_date'],
                        row['duration'],
                        row['views'],
                        row['likes'],
                        row['dislikes'],
                        row['comments'],
                        row['favourite_count'],
                        row['caption_status'])
                
        
                cursor.execute(insert_query,values)
                mydb.commit()


# comment table 

def Comments_Table(channel_names):

        mydb = psycopg2.connect(host='localhost',
                                user= 'postgres',
                                password = 'santhi@1998',
                                database = 'youtube_data',
                                port='5432')
        cursor = mydb.cursor()


        create_query = '''create table if not exists comments(comment_id varchar(100) primary key,
                                video_id varchar(50),
                                comment_text text,
                                comment_author varchar(150),
                                comment_published_at timestamp )'''
        cursor.execute(create_query)
        mydb.commit()

        individual_comments_details = []
        db = client["YTB_DATA"]
        coll1 = db["Channel_Details"]
        for cm_data in coll1.find({"Channel_Informations.channel_name":channel_names},{"_id":0}):
                        individual_comments_details.append(cm_data['Comment_Informations'])
        df_unique_cm = pd.DataFrame(individual_comments_details[0])

        for index,row in df_unique_cm.iterrows():
                        insert_query = '''insert into comments(comment_id,
                                video_id,
                                comment_text,
                                comment_author,
                                comment_published_at) 
                                                                
                                        values(%s,%s,%s,%s,%s)'''

                        values = (row['comment_id'],
                                row['video_id'],
                                row['comment_text'],
                                row['comment_author'],
                                row['comment_published_at'])
                        
                        cursor.execute(insert_query,values)
                        mydb.commit()


# add all tables in one function 


def Tables(unique_channel):
    
    result = Channels_Table(unique_channel) 
    if result:
            return result
    else:
            Playlist_Table(unique_channel)
            Videos_Table(unique_channel)
            Comments_Table(unique_channel)
    
            return 'Tables are created successfully'

def show_channels_table():
    ch_list = []
    db = client["YTB_DATA"]
    coll1 = db["Channel_Details"]
    for ch_data in coll1.find({},{"_id":0,"Channel_Informations":1}):
        ch_list.append(ch_data["Channel_Informations"])
    df = st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list = []
    db = client["YTB_DATA"]
    coll1 = db["Channel_Details"]
    for pl_data in coll1.find({},{"_id":0,"Playlist_Informations":1}):
                for i in range(len(pl_data['Playlist_Informations'])):
                        pl_list.append(pl_data['Playlist_Informations'][i])
    df1 = st.dataframe(pl_list)
    
    return df1


def show_videos_table():
    vi_list = []
    db = client["YTB_DATA"]
    coll1 = db["Channel_Details"]
    for vi_data in coll1.find({},{"_id":0,"Video_Informations":1}):
        for i in range(len(vi_data['Video_Informations'])):
                vi_list.append(vi_data['Video_Informations'][i])
    df2 = st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    cm_list = []
    db = client["YTB_DATA"]
    coll1 = db["Channel_Details"]
    for cm_data in coll1.find({},{"_id":0,"Comment_Informations":1}):
        for i in range(len(cm_data['Comment_Informations'])):
                cm_list.append(cm_data['Comment_Informations'][i])
    df3 = st.dataframe(cm_list)
    
    return df3

# streamlit code

with st.sidebar:
     st.title(":orange[Streamlit Application]")
     st.header("How to operate this?")
     st.caption("Select the channel id from youtube and paste it on particular tab")
     st.caption("insert the data into MongoDB")
     st.caption("Migrate the data to SQL")
     st.caption("Select the question from the tab")
     st.caption("Answer will be display")

st.header(":orange[Welcome to Streamlit]")
st.title(":orange[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
Channel_ID = st.text_input("Enter the Channel ID")

if st.button("Collect and store Data into MongoDB"):
          ch_ids=[]
          db=client['YTB_DATA']
          coll1=db['Channel_Details']
          for data in coll1.find({},{"_id":0,"Channel_Informations":1}):
               ch_ids.append(data["Channel_Informations"]["channel_id"])

          if Channel_ID in ch_ids:
               st.success("Details of the the given channel id is already exists")
          else:
                insert = Channel_Details(Channel_ID)
                st.success(insert)

ch_name = []
db = client["YTB_DATA"]
coll1 = db["Channel_Details"]
for ch_data in coll1.find({},{"_id":0,"Channel_Informations":1}):
    ch_name.append(ch_data['Channel_Informations']['channel_name'])

select_channel = st.selectbox("Select the Particular Channel",ch_name)


if st.button("Migrate to SQL"):
   table = Tables(select_channel)
   st.success(table)

show_table = st.radio("Select the table for view",("Channels","Playlists","Videos","Comments"))

if show_table == "Channels":
     show_channels_table() 

elif show_table == "Playlists":
     show_playlists_table()

elif show_table == "Videos":
     show_videos_table() 

elif show_table == "Comments":
     show_comments_table()  


# sql connections 


mydb = psycopg2.connect(host='localhost',
                        user= 'postgres',
                        password = 'santhi@1998',
                        database = 'youtube_data',
                        port='5432')
cursor = mydb.cursor()

questions = st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
"2.Which channels have the most number of videos, and how many videos do they have?",
"3.What are the top 10 most viewed videos and their respective channels?",
"4.How many comments were made on each video, and what are their corresponding video names?",
"5.Which videos have the highest number of likes, and what are their corresponding channel names?",
"6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
"7.What is the total number of views for each channel, and what are their corresponding channel names?",
"8.What are the names of all the channels that have published videos in the year 2022?",
"9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
"10.Which videos have the highest number of comments, and what are their corresponding channel names?"
))



# sql queries 10

mydb = psycopg2.connect(host='localhost',
                        user= 'postgres',
                        password = 'santhi@1998',
                        database = 'youtube_data',
                        port='5432')
cursor = mydb.cursor()

# first question 
if questions == "1.What are the names of all the videos and their corresponding channels?":
    query1 = '''select title as videos,channel_name as channel_name from videos'''
    cursor.execute(query1)
    mydb.commit()
    table1=cursor.fetchall()
    df1 = pd.DataFrame(table1,columns=['video title','channel name'])
    st.write(df1)

#second question 
elif questions == "2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select channel_name as channel_name,total_videos as no_of_videos from channels 
                             order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    table2=cursor.fetchall()
    df2 = pd.DataFrame(table2,columns=['channel name','no of videos'])
    st.write(df2)

#third question 

elif questions == "3.What are the top 10 most viewed videos and their respective channels?":
     query3 = '''select views as views, channel_name as channel_name, title as video_title from videos where views is not null order by views desc limit 10'''
     cursor.execute(query3)
     mydb.commit()
     table3=cursor.fetchall()
     df3 = pd.DataFrame(table3,columns=['views','channel name','video title'])
     st.write(df3)

# 4 th question
elif questions == "4.How many comments were made on each video, and what are their corresponding video names?":
        query4 = '''select comments as no_of_comments, title as video_title from videos where comments is not null'''
        mydb.commit()
        cursor.execute(query4)
        table4=cursor.fetchall()
        df4 = pd.DataFrame(table4,columns=['no of comments','video title'])
        st.write(df4)

# 5 th question 
elif questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
     query5 = '''select title as video_title,channel_name as channel_name,likes as like_count
                 from videos where likes is not null order by likes desc'''
     cursor.execute(query5)
     table5=cursor.fetchall()
     df5 = pd.DataFrame(table5,columns=['video title','channel name','like count'])
     st.write(df5)

# 6th question 
elif questions == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
      query6 = '''select likes as like_count,dislikes as dislike_count,title as video_title from videos'''
      cursor.execute(query6)
      table6=cursor.fetchall()
      df6 = pd.DataFrame(table6,columns=['like count','dislike count','video title'])
      st.write(df6)

# 7 th question 
elif questions == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
     query7 = '''select channel_name as channel_name,views as total_views from channels'''
     cursor.execute(query7)
     table7=cursor.fetchall()
     df7 = pd.DataFrame(table7,columns=['channel name','total views'])
     st.write(df7)


# 8 th question 
elif questions == "8.What are the names of all the channels that have published videos in the year 2022?":
     query8 = '''select title as video_title,published_date as published_date,channel_name as channel_name from videos where extract(year from published_date)=2022'''
     cursor.execute(query8)
     table8=cursor.fetchall()
     df8 = pd.DataFrame(table8,columns=['video title','published date','channel name'])
     st.write(df8)

# 9 th question 
elif questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": 
     query9 = '''select channel_name as channel_name,AVG(duration) as average_duration from videos group by channel_name'''
     cursor.execute(query9)
     table9=cursor.fetchall()
     df9 = pd.DataFrame(table9,columns=['channel name','average duration'])
     Table9 = []
     for index,row in df9.iterrows():
          channel_title = row['channel name']
          average_duration = row['average duration']
          average_duration_string = str(average_duration)
          Table9.append(dict(channeltitle = channel_title, averageduration = average_duration_string))
     dff9=pd.DataFrame(Table9)
     st.write(dff9)

# 10th question 
if questions == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
   query10 = '''select title as video_title,channel_name as channel_name,comments as comments from videos where comments is not null order by comments desc'''
   cursor.execute(query10)
   table10=cursor.fetchall()
   df10 = pd.DataFrame(table10,columns=['video title','channel name','comments'])
   st.write(df10)
