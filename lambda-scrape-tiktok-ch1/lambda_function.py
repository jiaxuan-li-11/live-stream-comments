import csv
from datetime import datetime
from TikTokLive import TikTokLiveClient
from TikTokLive.types.events import CommentEvent, ConnectEvent, GiftEvent, ShareEvent, LikeEvent, FollowEvent, ViewerUpdateEvent
import boto3 
import pytz

s3 = boto3.client("s3")
timezone = pytz.timezone('America/New_York')

videoID = "karaprens.58"
time_interval = 5

def create_s3(localpath, bucketpath, videoID, indicator):
    if indicator == 'comment':
        field_names= ['datetime', 'author', 'message']
    if indicator == 'viewer':
        field_names= ['datetime', 'number of viewers']
        
    with open(localpath, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
    s3.upload_file(localpath, "data-in-glue-bucket", bucketpath)
    
    
def update_s3(localpath, bucketpath, data, videoID, indicator):
    try:
        if indicator == 'comment':
            field_names= ['datetime', 'author', 'message']
        if indicator == 'viewer':
            field_names= ['datetime', 'number of viewers']
            
        s3.download_file("data-in-glue-bucket", bucketpath, localpath)
        with open(localpath, 'a', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writerows(data)
        s3.upload_file(localpath, "data-in-glue-bucket", bucketpath)
    except:
        create_s3(localpath, bucketpath, videoID, indicator)
        
localpath_current = '/tmp/current.csv'
localpath_history = '/tmp/history.csv'
bucketpath_current = 'tiktok/'+str(videoID)+'/current.csv'
bucketpath_history = 'tiktok/'+str(videoID)+'/history.csv'

localpath_current_viewer = '/tmp/current_viewer.csv'
localpath_history_viewer = '/tmp/history_viewer.csv'
bucketpath_current_viewer = 'tiktok/'+str(videoID)+'/current_viewer.csv'
bucketpath_history_viewer = 'tiktok/'+str(videoID)+'/history_viewer.csv'

client: TikTokLiveClient = TikTokLiveClient(unique_id=videoID)
data = []
start_time = datetime.now()
start_time_2 = datetime.now()
viewer_data = []

@client.on("connect")
async def on_connect(_: ConnectEvent):
    print("Connected to Room ID:", client.room_id)

@client.on("comment")
async def on_comment(event: CommentEvent):
    global start_time, data
    data.append({'datetime':datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S"), "author":event.user.nickname, "message":event.comment})
    curr_time = datetime.now()
    duration_in_s = (curr_time-start_time).total_seconds() 
    duration_in_min = divmod(duration_in_s, 60)[0] 
    # print(duration_in_s)
    
    if duration_in_min >= time_interval:
        create_s3(localpath_current, bucketpath_current, videoID, 'comment')

        update_s3(localpath_current, bucketpath_current, data, videoID, 'comment')
        update_s3(localpath_history, bucketpath_history, data, videoID, 'comment')

        print(data)
        data = []
        start_time = datetime.now()
        
@client.on("viewer_update")
async def on_connect(event: ViewerUpdateEvent):
    global start_time_2, viewer_data
    viewer_data.append({'datetime':datetime.now(timezone).strftime("%Y-%m-%d %H:%M:%S"), "number of viewers":event.viewer_count}) 
    curr_time = datetime.now()
    duration_in_s = (curr_time-start_time_2).total_seconds() 
    duration_in_min = divmod(duration_in_s, 60)[0]
    
    if duration_in_min >= time_interval:
        create_s3(localpath_current_viewer, bucketpath_current_viewer, videoID, 'viewer')
        
        update_s3(localpath_current_viewer, bucketpath_current_viewer, viewer_data, videoID, 'viewer')
        update_s3(localpath_history_viewer, bucketpath_history_viewer, viewer_data, videoID, 'viewer')
        
        print(viewer_data)
        viewer_data = []
        start_time_2 = datetime.now()

def lambda_handler(event, context): 
    client.run()
    return 
