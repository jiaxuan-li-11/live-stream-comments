import boto3
import csv
import json 
from botocore.config import Config

config = Config(
   retries = {
      'max_attempts': 50,
      'mode': 'standard'
   }
)

s3 = boto3.client('s3', config=config)

def convert_csv_to_list(file_resp):
    
    # Parse the CSV file contents
    csv_data = []
    csv_reader = csv.DictReader(file_resp['Body'].read().decode('utf-8').splitlines())
    for row in csv_reader:
        csv_data.append(row)
        
    output = {'Content': csv_data}
    return output

def lambda_handler(event, context):
    print(event)
    # Define the bucket name and key for the CSV file
    filename = event['queryStringParameters']['q']
    bucket_name = 'data-out-glue-bucket-1'
    csv_key = filename
    
    print(filename)
    
    history_emoji = []
    history_negative = []
    history_neutral = []
    history_positive = []
    history_words = []
    history_language = []
    history_sentiment = []
    
    current_emoji = []
    current_negative = []
    current_neutral = []
    current_positive = []
    current_words = []
    current_language = []
    current_sentiment = []

    contents = s3.list_objects_v2(Bucket=bucket_name,Prefix=csv_key)['Contents']
    for content in contents:
        key = content['Key']
        if 'history_emoji' in key:
            history_emoji = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'history_negative' in key:
            history_negative = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'history_neutral' in key:
            history_neutral = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'history_positive' in key:
            history_positive = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'history_words' in key:
            history_words = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'history_language' in key:
            history_language = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'history_sentiment' in key:
            history_sentiment = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_emoji' in key:
            current_emoji = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_negative' in key:
            current_negative = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_neutral' in key:
            current_neutral = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_positive' in key:
            current_positive = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_words' in key:
            current_words = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_language' in key:
            current_language = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
        elif 'current_sentiment' in key:
            current_sentiment = convert_csv_to_list(s3.get_object(Bucket=bucket_name, Key=key))
            
    total_comments_history = []
    print(history_neutral)
    for i in range(len(history_neutral['Content'])):
        total_comments_start = history_neutral['Content'][i]['window_start']
        # print(history_neutral['Content'][i]['count'], history_positive['Content'][i]['count'], history_negative['Content'][i]['count'])
        total_comments_count = int(history_neutral['Content'][i]['count']) + int(history_positive['Content'][i]['count']) + int(history_negative['Content'][i]['count'])
        # print(total_comments_count)
        total_comments_history.append({"window_start":total_comments_start, "count":total_comments_count})
        # print(total_comments_history)
    comments_history = {"Content":total_comments_history} 


    output = {
            "history_emoji":history_emoji,
            "history_negative":history_negative,
            "history_neutral":history_neutral,
            "history_positive":history_positive,
            "history_words":history_words,
            "history_language":history_language,
            "history_sentiment":history_sentiment,
            "history_comments":comments_history,
            
            "current_emoji":current_emoji,
            "current_negative":current_negative,
            "current_neutral":current_neutral,
            "current_positive":current_positive,
            "current_words":current_words,
            "current_language":current_language,
            "current_sentiment":current_sentiment
        }
    

    # Return the search results as an API Gateway response
    return {
        "statusCode": 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'
        },
        "body": json.dumps(output)
    }