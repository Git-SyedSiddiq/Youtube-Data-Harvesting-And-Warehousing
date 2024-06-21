# creating youtube connection
import googleapiclient.discovery

api_service_name='youtube'
api_version='v3'
api_key='AIzaSyAm3QQZLrw826tF4cR6H4A1f-zcnsWQnoM'

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api_key)

# importing libraries
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import datetime
from datetime import timedelta


# connection to mongodb
client=pymongo.MongoClient('mongodb://localhost:27017')
db=client['Youtubedata']
coll=db['channel_details']

# getting channel data

def channel_data(c_id):
    request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=c_id)
    response = request.execute()
    

    res={'ch_name':response['items'][0]['snippet']['title'],
    'ch_id':response['items'][0]['id'],
    'ch_des':response['items'][0]['snippet']['description'],
    'ch_playlist':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    'ch_viewcount':response['items'][0]['statistics']['viewCount'],
    'ch_subcount':response['items'][0]['statistics']['subscriberCount'],
    'ch_videocount':response['items'][0]['statistics']['videoCount']}
      
    return res



# getting video_ids
def getting_video_ids(ch_id):
    request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=ch_id)
    response = request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    video_ids=[]
    next_page_token=None
    while True:
        request = youtube.playlistItems().list(
                part="snippet",
                maxResults=50,
                playlistId=playlist_id,
                pageToken=next_page_token
            )
        response = request.execute()
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids



# getting video details
def getting_video_details(video_ids):
    video_details=[]
    for j in video_ids:
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=j)
        response = request.execute()

        duration=response['items'][0]['contentDetails']['duration']
        duration=duration[2:]
        time=timedelta()
        if 'H' in duration:
            hours=int(duration.split('H')[0])
            time+=timedelta(hours=hours)
            duration=duration.split('H')[1]

        if 'M' in duration:
            minutes=int(duration.split('M')[0])
            time+=timedelta(minutes=minutes)
            duration=duration.split('M')[1]

        if 'S' in duration:
            seconds=int(duration.split('S')[0])
            time+=timedelta(seconds=seconds)
        total_seconds=int(time.total_seconds())

        data={'channel_name':response['items'][0]['snippet']['channelTitle'],
            'channel_id':response['items'][0]['snippet']['channelId'],
            'video_id':response['items'][0]['id'],
            'video_name':response['items'][0]['snippet']['title'],
            'published_at':response['items'][0]['snippet']['publishedAt'],
            'channel_desc':response['items'][0]['snippet']['description'],
            'view_count':response['items'][0]['statistics']['viewCount'],
            'like_count':response['items'][0]['statistics'].get('likeCount'),
            'fav_count':response['items'][0]['statistics']['favoriteCount'],
            'comm_count':response['items'][0]['statistics']['commentCount'],
            'duration':total_seconds,
            'definition':response['items'][0]['contentDetails']['definition'],
            'caption':response['items'][0]['contentDetails']['caption']}
       
        video_details.append(data)
    return video_details


# getting comment details

def comment_data(video_ids):
    comment_details=[]
    next_page_token=None
    while True:
        i=0
        try:
            for id in video_ids:
                request = youtube.commentThreads().list(part="snippet",videoId=id,maxResults=50,pageToken=next_page_token)
                response = request.execute()

                for item in response['items']:
                    data = {'cmmt_id': item['snippet']['topLevelComment']['id'],
                        'video_id': item['snippet']['videoId'],
                        'cmmt':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'cmmtor_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'commtor_publishedat':item['snippet']['topLevelComment']['snippet']['publishedAt']}
                    
                    comment_details.append(data)
            next_page_token=response.get('nextPagetoken')
            i=i+1
            if i==2:
                break

        except:
            pass
        return comment_details

# getting playlist ids
def getting_playlist_id(c_id):
    playlist_list=[]
    next_page_token=None
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",maxResults=50,
            channelId=c_id,pageToken=next_page_token)
        response = request.execute()
        for i in response['items']:
            data = {'ch_name': i['snippet']['channelTitle'],
                'playlst_id': i['id'],
                'Published_at': i['snippet']['publishedAt'],
                'ch_id': i['snippet']['channelId'],
                'title': i['snippet']['title'],
                'video_count': i['contentDetails']['itemCount']}
            playlist_list.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_list



def channel_details(c_id):
    channel_info=channel_data(c_id)
    video_ids_info=getting_video_ids(c_id)
    videos_info=getting_video_details(video_ids_info)
    comments_info=comment_data(video_ids_info)
    playlist_info=getting_playlist_id(c_id)

    coll.insert_one({'channel_information':channel_info,'video_ids':video_ids_info,'video_information':videos_info,
                 'comments_information':comments_info,'playlist_information':playlist_info})
    return 'inserted to mongodb succesfully'


#creating channels tables
def channels_tables(ch):
    connection=mysql.connector.connect(host='localhost',port='3306',user='root',password="1234",database='youtube')
    cursor=connection.cursor()
    try:
        query='''create table if not exists channel_details (ch_name varchar(100),ch_id varchar(100) primary key,ch_des text,
                ch_playlist varchar(80),ch_viewcount bigint,ch_subcount bigint,ch_videocount int)'''
        cursor.execute(query)
        connection.commit()
    except:
        print('channels table already created')

    channel_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for i in coll.find({'channel_information.ch_name':ch},{'_id':0}):
        channel_list.append(i['channel_information'])

    df=pd.DataFrame(channel_list)

    for index,row in df.iterrows():
        insertion_query ='''insert into channel_details (ch_name,
                                                        ch_id,
                                                        ch_des,
                                                        ch_playlist,
                                                        ch_viewcount,
                                                        ch_subcount,
                                                        ch_videocount)
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
        values= (row['ch_name'],
                row['ch_id'],
                row['ch_des'],
                row['ch_playlist'],
                row['ch_viewcount'],
                row['ch_subcount'],
                row['ch_videocount'])
        
        try:
            cursor.execute(insertion_query,values)
            connection.commit()
        except:
            message= f'The channel name {ch} is already migrated to sql'
            return message


# creating playlists table
def playlists_table(ch):
    connection=mysql.connector.connect(host='localhost',port='3306',user='root',password="1234",database='youtube')
    cursor=connection.cursor()

    try:
        query='''create table if not exists playlists (ch_name varchar(100),
                                                        playlst_id varchar(100) primary key,
                                                        Published_at timestamp,
                                                        ch_id varchar(80),
                                                        title text,
                                                        video_count bigint)'''
        cursor.execute(query)
        connection.commit()
    except:
        print('playlists table already created')

    pl_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for j in coll.find({'channel_information.ch_name':ch},{'_id':0}):
            pl_list.append(j['playlist_information'])
                
    df1=pd.DataFrame(pl_list[0])

    for index,row in df1.iterrows():
        insertion_query ='''insert into playlists (ch_name,
                                                    playlst_id,
                                                    Published_at,
                                                    ch_id,
                                                    title,
                                                    video_count)
                                                    values(%s,%s,%s,%s,%s,%s)'''
        values= (row['ch_name'],
                row['playlst_id'],
                datetime.datetime.strptime(row['Published_at'],'%Y-%m-%dT%H:%M:%SZ'),
                row['ch_id'],
                row['title'],
                row['video_count'])
        cursor.execute(insertion_query,values)
        connection.commit()



# creating videos table
def videos_table(ch):
    connection=mysql.connector.connect(host='localhost',port='3306',user='root',password="1234",database='youtube')
    cursor=connection.cursor()
    try:
        query='''create table if not exists video (channel_name varchar(100),
                                                        channel_id varchar(100),
                                                            video_id varchar(100) primary key,
                                                            video_name text,
                                                            published_at timestamp,
                                                            channel_desc text,
                                                            view_count int,
                                                            like_count int,
                                                            fav_count int,
                                                            comm_count int,
                                                            duration int,
                                                            definition varchar(10),
                                                            caption varchar(30))'''
        cursor.execute(query)
        connection.commit()
    except:
        print('video table already created')

    videos_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for v in coll.find({'channel_information.ch_name':ch},{'_id':0}):
        videos_list.append(v['video_information'])
    

    df_vid=pd.DataFrame(videos_list[0])

    for index,row in df_vid.iterrows():
        insertion_query ='''insert into video(channel_name ,
                                                channel_id,
                                                video_id,
                                                video_name,
                                                published_at,
                                                channel_desc,
                                                view_count,
                                                like_count,
                                                fav_count,
                                                comm_count,
                                                duration,
                                                definition,
                                                caption)
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = ( row['channel_name'],
                row['channel_id'],
                row['video_id'],
                row['video_name'],
                datetime.datetime.strptime(row['published_at'],'%Y-%m-%dT%H:%M:%SZ'),
                row['channel_desc'],
                row['view_count'],
                row['like_count'],
                row['fav_count'],
                row['comm_count'],
                row['duration'],
                row['definition'],
                row['caption'])
        cursor.execute(insertion_query,values)
        connection.commit()


# creating comments table
def comments_table(ch):
    connection=mysql.connector.connect(host='localhost',port='3306',user='root',password="1234",database='youtube')
    cursor=connection.cursor()
    
    try:
        query='''create table if not exists comments (cmmt_id varchar(100) primary key,
                                                    video_id varchar(100),
                                                        cmmt text,
                                                        cmmtor_name varchar(100),
                                                        commtor_publishedat varchar(100)
                                                        )'''                  
        cursor.execute(query)
        connection.commit()
    except:
        print('comments table already created')

    comments_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for c in coll.find({'channel_information.ch_name':ch},{'_id':0}):
        comments_list.append(c['comments_information'])
    

    df_comm=pd.DataFrame(comments_list[0])

    for index,row in df_comm.iterrows():
        insertion_query ='''insert into comments (cmmt_id ,
                                                video_id,
                                                cmmt,
                                                cmmtor_name,
                                                commtor_publishedat)
                                                values(%s,%s,%s,%s,%s)'''
        values = ( row['cmmt_id'],
        row['video_id'],
        row['cmmt'],
        row['cmmtor_name'],
        row['commtor_publishedat'])
        cursor.execute(insertion_query,values)
        connection.commit()


# creating all tables at a time 
def youtube_tables(ch):
    message=channels_tables(ch) 
    if message:
        return message
    else:
        playlists_table(ch)
        videos_table(ch)
        comments_table(ch)
        return 'Tables created successfully'


def show_channels_table():
    channel_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for i in coll.find({},{'_id':0,'channel_information':1}):
        channel_list.append(i['channel_information'])
    df=st.dataframe(channel_list)
    return df

def show_playlists_table():
    pl_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for j in coll.find({},{'_id':0,'playlist_information':1}):
            for k in range (len(j['playlist_information'])):
                pl_list.append(j['playlist_information'][k])
    df1=st.dataframe(pl_list)
    return df1

def show_videos_table():
    videos_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for v in coll.find({},{'_id':0,'video_information':1}):
            for v1 in range (len(v['video_information'])):
                videos_list.append(v['video_information'][v1])
    df_vid=st.dataframe(videos_list)
    return df_vid

def show_comments_table():
    comments_list=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for c in coll.find({},{'_id':0,'comments_information':1}):
            for c1 in range (len(c['comments_information'])):
                comments_list.append(c['comments_information'][c1])
    df_comm=st.dataframe(comments_list)
    return df_comm




# coding for streamlit
with st.sidebar:
    st.title('Youtube Data Harvesting and Warehousing')
    st.header('Tools used:')
    st.caption('Python')
    st.caption('MongoDB')
    st.caption('MySQL')

channel_id=st.text_input('Enter the channel ID')

if st.button('Collect and store data'):
    ch_ids=[]
    db=client['Youtubedata']
    coll=db['channel_details']
    for id in coll.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(id['channel_information']['ch_id'])
    
    if channel_id in ch_ids:
        st.success('Channel details already collected and stored')
    else:
        insert=channel_details(channel_id)
        st.success(insert)

# migrating particluar channel to MySql
ch_lst=[]
db=client['Youtubedata']
coll=db['channel_details']
for i in coll.find({},{'_id':0,'channel_information':1}):
    ch_lst.append(i['channel_information']['ch_name'])
ch=st.selectbox('Select the channel name',(ch_lst))



if st.button('Migrate to MySQL'):
    migrate=youtube_tables(ch)
    st.success(migrate)

table_name=st.radio('Select the table to view',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))
if table_name=='CHANNELS':
    show_channels_table()

elif table_name=='PLAYLISTS':
    show_playlists_table()

elif table_name=='VIDEOS':
    show_videos_table()

elif table_name=='COMMENTS':
    show_comments_table()


# MySQL connection
connection=mysql.connector.connect(host='localhost',port='3306',user='root',password="1234",database='youtube')
cursor=connection.cursor()
questions=st.selectbox('Select the Question',
                    ('1.What are the names of all the videos and their corresponding channels?',
                    '2.Which channels have the most number of videos, and how many videos do they have?',
                    '3.What are the top 10 most viewed videos and their respective channels?',
                    '4.How many comments were made on each video, and what are their corresponding video names?',
                    '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                    '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                    '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                    '8.What are the names of all the channels that have published videos in the year 2022?',
                    '9.What is the average duration of all videos in channel, and what are their corresponding channel names?',
                    '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))
                        
if questions=='1.What are the names of all the videos and their corresponding channels?':
    query1= '''select channel_name ,video_name from video'''
    cursor.execute(query1)
    names=cursor.fetchall()
    df1=pd.DataFrame(names,columns=['channel name','video name'])
    st.write(df1)

elif  questions=='2.Which channels have the most number of videos, and how many videos do they have?':
    query2='''select channel_name,count(video_id) from video group by channel_name limit 1'''
    cursor=connection.cursor()
    cursor.execute(query2)
    no_of_videos=cursor.fetchall()
    df2=pd.DataFrame(no_of_videos,columns=['channel name','no_of_videos'])
    st.write(df2)

elif questions=='3.What are the top 10 most viewed videos and their respective channels?':
    query3='''select channel_name,video_name from video order by view_count desc limit 10;'''
    cursor=connection.cursor()
    cursor.execute(query3)
    top_viewed=cursor.fetchall()
    df3=pd.DataFrame(top_viewed,columns=['channel name','most viewed'])
    st.write(df3)

elif questions=='4.How many comments were made on each video, and what are their corresponding video names?':
    query4='''select video_name,comm_count from video;'''
    cursor.execute(query4)
    no_of_comments=cursor.fetchall()
    df4=pd.DataFrame(no_of_comments,columns=['video_name','comments count'])
    st.write(df4)

elif questions=='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5='''select video_name,channel_name,like_count from video order by like_count desc'''
    cursor.execute(query5)
    most_liked=cursor.fetchall()
    df5=pd.DataFrame(most_liked,columns=['video name','channel name','most liked'])
    st.write(df5)

elif questions=='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6='''select video_name,like_count from video order by like_count desc'''
    cursor.execute(query6)
    most_liked=cursor.fetchall()
    df6=pd.DataFrame(most_liked,columns=['video name','likes count'])
    st.write(df6)

elif questions=='7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7='''select channel_name,sum(view_count) from video group by channel_name'''
    cursor.execute(query7)
    views_count=cursor.fetchall()
    df7=pd.DataFrame(views_count,columns=['channel name','total views'])
    st.write(df7)

elif questions=='8.What are the names of all the channels that have published videos in the year 2022?':
    query8='''select channel_name,video_name,published_at from video where extract(year from published_at)=2022'''
    cursor.execute(query8)
    published_2022=cursor.fetchall()
    df8=pd.DataFrame(published_2022,columns=['channel name','video name','published at'])
    st.write(df8)

elif questions=='9.What is the average duration of all videos in channel, and what are their corresponding channel names?':
    query9='''select channel_name, round(avg(duration)/60) from video group by channel_name''' 
    cursor.execute(query9)
    avg_duration=cursor.fetchall()
    df9=pd.DataFrame(avg_duration,columns=['channel name','average duration in min'])
    st.write(df9)

elif questions=='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10='''select video_name,channel_name,comm_count from video order by comm_count desc'''
    cursor.execute(query10)
    comm_count=cursor.fetchall()
    df10=pd.DataFrame(comm_count,columns=['video name','channel name','most commented'])
    st.write(df10)