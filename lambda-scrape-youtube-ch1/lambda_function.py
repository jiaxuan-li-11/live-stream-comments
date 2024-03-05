import csv
import requests
from datetime import datetime
import boto3 
import pytchat
import botocore
import pytz
import ast

timezone = pytz.timezone('America/New_York')

s3 = boto3.client("s3")
videoID = "36YnV9STBqc"
time_interval = 5


localpath_current = '/tmp/current.csv'
localpath_history = '/tmp/history.csv'
bucketpath_current = 'youtube/'+str(videoID)+'/current.csv'
bucketpath_history = 'youtube/'+str(videoID)+'/history.csv'

def create_s3(localpath, bucketpath, videoID):
    field_names= ['datetime', 'author', 'message']
    with open(localpath, 'w', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
    s3.upload_file(localpath, "data-in-glue-bucket", bucketpath)
    
    
def update_s3(localpath, bucketpath, data, videoID):
    try: 
        field_names= ['datetime', 'author', 'message']
        s3.download_file("data-in-glue-bucket", bucketpath, localpath)
        with open(localpath, 'a', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writerows(data)
        s3.upload_file(localpath, "data-in-glue-bucket", bucketpath)
    except:
        create_s3(localpath_current, bucketpath_current, videoID)
        create_s3(localpath_history, bucketpath_history, videoID)
        
        
def get_emoji(message):
    text = ''
    if '{\'id\': \'' in (f"{message}"):
        regex_pattern = r"'id': '(.){1}"
        my_string = f"{message}"
        dic = ast.literal_eval(my_string)
        data = ''
        for i in dic:
            try:
                data += (i['id'])
            except:
                data += i
            text = '[\''+data+'\']'
    else:
        text = f"{message}"
    return text[2:-2]

def lambda_handler(event, context): 
    data = []
    chat = pytchat.create(video_id=videoID)

    start_time = datetime.now(timezone)

    while chat.is_alive():
        for c in chat.get().sync_items():
            line = {'datetime':datetime.now(timezone).strftime("%Y/%m/%d %H:%M"), "author":c.author.name, "message":get_emoji(c.messageEx)}
            data.append(line)
            print(line)
            curr_time = datetime.now(timezone)
            duration_in_s = (curr_time-start_time).total_seconds() 
            duration_in_min = divmod(duration_in_s, 60)[0] 
            

            if duration_in_min >= time_interval:
                
                print(data)
                create_s3(localpath_current, bucketpath_current, videoID)
                update_s3(localpath_current, bucketpath_current, data, videoID)
                update_s3(localpath_history, bucketpath_history, data, videoID)
                print("upload to s3 bucket")
        
                data = []
                start_time = datetime.now(timezone)
                
                    

