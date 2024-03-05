import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StringType, ArrayType


from langdetect import detect
from datetime import datetime, timedelta
import emoji
import nltk
from textblob import TextBlob
import re


nltk.download('stopwords')  # list of stopping words
# word count
# Get the English stopwords list from NLTK
stopwords = set(nltk.corpus.stopwords.words('english'))

spark = SparkSession.builder.getOrCreate()

args = getResolvedOptions(sys.argv, ["file","bucket", "status"])
file_name=args['file']
bucket_name=args['bucket']
file_status = args['status']

print("Bucket Name" , bucket_name)
print("File Name" , file_name)
input_file_path="s3://{}/{}/{}.csv".format(bucket_name,file_name, file_status)

print("Input File Path : ",input_file_path)
df = spark.read.option("inferSchema", False).option("header", "true").csv(input_file_path)

comments = spark.read.csv(input_file_path, header=True)
comments_only = comments[['message']]

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

# language 用的是filter掉emoji之后的
def getLang(val):
    # translator = Translator(service_urls=['translate.google.com', 'translate.google.co.kr'])
    try:
        language_detected = detect(str(val))
        # language_detected = translator.detect(str(val)).lang
    except:
        language_detected = 'NDN'
    return language_detected
udf_lang = udf(getLang, StringType())

df_lang = comments_noemoji.withColumn("Bytes", udf_lang(comments_noemoji["message"]))
language_count = df_lang[['Bytes']].groupBy('Bytes').count().orderBy('count', ascending=False)

language_count_10 = language_count.limit(10)

path_language = "s3://data-out-glue-bucket-1/{}/{}_language".format(file_name, file_status)

language_count_10.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_language)

# define a UDF to get rid of meaningless words
def clean_message(message):
    try:
        words = re.findall(r'\b\w+\b', message.lower())
        words_en = [w for w in words if w not in stopwords]
        return words_en
    except:
        return 'NDN'

clean_message_udf = udf(clean_message, ArrayType(StringType()))

# add a new column containing the language of each sentence
# df_lang2 = comments.withColumn("lang", classify_lang_udf(comments["message"]))

# filter out non-English sentences
df_en = df_lang.filter(df_lang["Bytes"] == "en")

words = df_en.withColumn('words', clean_message_udf('message'))\
            .select(explode("words").alias("words"))\
            .groupBy("words").count().orderBy("count", ascending=False)
            
            
words = words.limit(50)

path_words = "s3://data-out-glue-bucket-1/{}/{}_words".format(file_name, file_status)

words.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(path_words)
