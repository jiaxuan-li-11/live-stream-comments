import json 
import boto3
from botocore.config import Config

glue = boto3.client("glue")
config = Config(
   retries = {
         'max_attempts': 50,
         'mode': 'standard'
      }
   )
s3 = boto3.client('s3', config=config)
bucket_name = 'data-in-glue-bucket'
csv_key = ''

      
def run_job(file, status, feature, packages):
   file_name = file
   job_name = "{}/{}/{}".format(file_name, status, feature)
   print(job_name)
   try:
      response_create = glue.create_job(
         Name=job_name,
         Role='arn:aws:iam::065099833513:role/live-stream-glue-role',
         ExecutionProperty={'MaxConcurrentRuns': 200}, 
         Command={
            'Name': 'glueetl', 
            'ScriptLocation': 's3://pyspark-scripts-j/{}.py'.format(feature), 
            'PythonVersion': '3'
         }, 
         DefaultArguments={
            '--enable-metrics': 'true', 
            '--enable-spark-ui': 'true', 
            '--spark-event-logs-path': 's3://aws-glue-assets-065099833513-us-east-1/sparkHistoryLogs/', 
            '--enable-job-insights': 'true', 
            '--additional-python-modules': packages, 
            '--enable-glue-datacatalog': 'true', 
            '--enable-continuous-cloudwatch-log': 'true', 
            '--job-bookmark-option': 'job-bookmark-disable', 
            '--job-language': 'python', 
            '--TempDir': 's3://aws-glue-assets-065099833513-us-east-1/temporary/'
         }, 
         MaxRetries=0, 
         Timeout= 2880,  
         WorkerType= 'G.1X', 
         NumberOfWorkers= 3, 
         GlueVersion= '3.0', 
         ExecutionClass= 'STANDARD',
         )
   except:
      pass
   print("job is created")

   response = glue.start_job_run(
         JobName=job_name, 
         Arguments={            
         "--file":file_name,
         "--bucket":bucket_name, 
         "--status":status}
         )



def lambda_handler(event, context):
   files = []
   for item in s3.list_objects_v2(Bucket=bucket_name,Prefix=csv_key)['Contents']:
      file_name = "{}/{}".format(item['Key'].split('/')[0], item['Key'].split('/')[1])
      if file_name not in files:
         files.append(file_name)
   for file in files:
      print(file) 
      run_job(file, 'history', 'language_word', 'emoji,nltk,textblob,langdetect')
      run_job(file, 'history', 'sentiment_emoji', 'emoji,nltk,textblob,advertools')


         


         
      

      
