import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, hour, col, expr
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType


from datetime import datetime, timedelta
import emoji
import re
import nltk
import advertools as adv
from textblob import TextBlob

import pytz
timezone = pytz.timezone('America/New_York')

nltk.download('stopwords')  # list of stopping words

spark = SparkSession.builder.getOrCreate()

args = getResolvedOptions(sys.argv, ["file","bucket", "status"])
file_name=args['file']
bucket_name=args['bucket']
file_status = args['status']

print("Bucket Name" , bucket_name)
print("File Name" , file_name)
input_file_path="s3://{}/{}/{}.csv".format(bucket_name,file_name, file_status)


print("Input File Path : ",input_file_path)
# df = spark.read.option("inferSchema", False).option("header", "true").csv(input_file_path)

comments = spark.read.csv(input_file_path, header=True)
comments_only = comments[['message']]

# TASK 1
# top emoji
def extract_emojis(s):
    try:
        emojis = adv.extract_emoji(s)['emoji_flat']
        return emojis
    except:
        return 'NDN'

extract_emojis_udf = udf(extract_emojis, ArrayType(StringType()))
emoji_count = comments.withColumn("emojis", extract_emojis_udf(comments["message"]))\
                .select(explode("emojis").alias("emoji"))\
                .groupBy("emoji").count().orderBy("count", ascending=False)

emoji_df = emoji_count.limit(10).orderBy("count", ascending=True)
# emoji_pandas_df.to_csv(path_emoji, index=False)


path_emoji = "s3://data-out-glue-bucket-1/{}/{}_emoji".format(file_name, file_status)
emoji_df.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_emoji)

# TASK 2
# replace emoji
def replace_emoji(text):
  try:
    output = emoji.replace_emoji(text,None)
  except:
    output = None
  return output
replace_emoji_UDF = udf(lambda x:replace_emoji(x), StringType())
comments_noemoji = comments.withColumn('message', replace_emoji_UDF(col('message')))
comments_noemoji = comments_noemoji.filter(length('message')>0)
if 'youtube' in file_name:
    comments_noemoji = comments_noemoji.withColumn('datetime', to_timestamp('datetime', 'yyyy/MM/dd HH:mm'))
    comments_noemoji = comments_noemoji.withColumn('datetime', date_format('datetime', 'yyyy-MM-dd HH:mm:ss'))


hours = 24 # T-axis for plot
window_size = "1 hour"  # window length for input data

comments_noemoji = comments_noemoji.filter(col('datetime') >= current_timestamp() - expr("INTERVAL 1 DAY"))


# sentiment analysis
def sentiment_analysis(text):
    try:
      analysis = TextBlob(text)
      return analysis.sentiment.polarity
    except:
        return 0

sentiment_UDF = udf(lambda x:sentiment_analysis(x),StringType()) 
comments_sen = comments_noemoji.withColumn('sentiment', sentiment_UDF(col('message')))

def sentiment_class(text):
  value = float(text)
  if value > 0:
    return 'positive'
  elif value < 0:
    return 'negative'
  else:
    return 'neutral'

sentiment_class_UDF = udf(lambda x:sentiment_class(x),StringType())
comments_sen = comments_sen.withColumn("sentiment_class", sentiment_class_UDF(col('sentiment')))
if file_status=='current':
    print('current')
    comment_current = comments_sen.select('sentiment_class')\
                .groupBy("sentiment_class").count().orderBy("count", ascending=False)
    sum_A = comment_current.select(sum('count')).collect()[0][0]
    comment_current = comment_current.withColumn('percent', comment_current['count']/sum_A)

    path_sentiment = "s3://data-out-glue-bucket-1/{}/{}_sentiment".format(file_name, file_status)
    comment_current.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_sentiment)
else:
    print('history')
    comments_sen_h = comments_sen[['datetime','sentiment_class']]
    comments_date = comments_sen_h.withColumn("time_window", window("datetime", window_size))
    tumblingwindow = comments_date.groupBy("time_window", "sentiment_class").agg(count("sentiment_class").alias("count")).orderBy('time_window')

    positive_df = tumblingwindow.filter(tumblingwindow.sentiment_class == "positive")
    negative_df = tumblingwindow.filter(tumblingwindow.sentiment_class == "negative")
    neutral_df = tumblingwindow.filter(tumblingwindow.sentiment_class == "neutral")

    positive_df = positive_df.withColumn('start',positive_df.time_window.start)
    positive_df = positive_df[['start','sentiment_class','count']]
    negative_df = negative_df.withColumn('start',negative_df.time_window.start)
    negative_df = negative_df[['start','sentiment_class','count']]
    neutral_df = neutral_df.withColumn('start',neutral_df.time_window.start)
    neutral_df = neutral_df[['start','sentiment_class','count']]

    # window template
    schema = StructType([
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
    ])

    # 创建一个空的DataFrame
    window_frame_template = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    # 获取当前时间的整数小时
    current_time = datetime.now(timezone).replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    current_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

    # 生成从当前时间的整数小时往前数24小时的时间范围
    for i in range(hours):
        window_start = current_time - timedelta(hours=i + 1)
        window_end = current_time - timedelta(hours=i)
        row = spark.createDataFrame([(window_start, window_end)], schema)
        window_frame_template = window_frame_template.union(row)

    # 排序窗口时间范围
    window_frame_template = window_frame_template.sort("window_start")

    # using template to scale
    positive_result = window_frame_template.join(positive_df,window_frame_template["window_start"] == positive_df["start"],"left").sort("window_start").withColumn('window_start', hour('window_start'))
    negative_result = window_frame_template.join(negative_df,window_frame_template["window_start"] == negative_df["start"],"left").sort("window_start").withColumn('window_start', hour('window_start'))
    neutral_result = window_frame_template.join(neutral_df,window_frame_template["window_start"] == neutral_df["start"],"left").sort("window_start").withColumn('window_start', hour('window_start'))

    positive_result = positive_result[['window_start', 'count']]
    negative_result = negative_result[['window_start', 'count']]
    neutral_result = neutral_result[['window_start', 'count']]
    positive_result = positive_result.na.fill({'count': '0'})
    negative_result = negative_result.na.fill({'count': '0'})
    neutral_result = neutral_result.na.fill({'count': '0'})
    # path_positive = "s3://data-out-glue-bucket-1/tiktok/bridgetleigh_/history_positive"
    # path_negative = "s3://data-out-glue-bucket-1/tiktok/bridgetleigh_/history_negative"
    # path_neutral = "s3://data-out-glue-bucket-1/tiktok/bridgetleigh_/history_neutral"


    path_positive = "s3://data-out-glue-bucket-1/{}/{}_positive".format(file_name, file_status)
    path_negative = "s3://data-out-glue-bucket-1/{}/{}_negative".format(file_name, file_status)
    path_neutral = "s3://data-out-glue-bucket-1/{}/{}_neutral".format(file_name, file_status)


    positive_result.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_positive)
    negative_result.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_negative)
    neutral_result.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_neutral)


