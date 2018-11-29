'''
This script includes some further clean-up on Spark for analysis.

'''


##====================================================================
#### Connect to Hive:
##====================================================================
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import Row

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql('show databases').show()

##====================================================================
#### Clean datetime value:
##====================================================================
from pyspark.sql.functions import unix_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, ArrayType, DoubleType

df_news.printSchema()
df_twitter.printSchema()

#Check news data 
#______________________________________________________
df_news.select('publishedAt').show(1)
split_col_date = F.split(df_news['publishedAt'], 'T')
df_news2 = df_news.withColumn('Date', split_col_date.getItem(0))
df_news2.select('Date').show(1)
df_news2 = df_news2.withColumn('PublishedDate',F.to_date(df_news2.Date))
df_news2 = df_news2.drop('Date')
df_news2.printSchema()

#Check twitter data 
#______________________________________________________
df_twitter.select('created_at').take(1)

def extract_datetime(datestring):
	date = datestring.split(' ')
	month = date[1]
	day = date[2]
	year = date[5]
	lookup_tbl = {'Jan':'01','Feb':'02', 'Mar':'03', 'Apr':'04','May':'05', 'Jun':'06','Jul':'07','Aug':'08', 'Sep':'09', 'Oct':'10','Nov':'11','Dec':'12'}
	month2 = lookup_tbl[month]
	return '-'.join([year, month2, day])

extract_datetime_udf = F.udf(lambda s: extract_datetime(s), StringType())
df_twitter2 = df_twitter.withColumn('DateStr', extract_datetime_udf(df_twitter.created_at))
df_twitter2.select('DateStr').take(1)
df_twitter2 = df_twitter2.withColumn('created_at_dt',F.to_date(df_twitter2.DateStr))
df_twitter2.select('created_at_dt').take(1)
df_twitter2.printSchema()


##====================================================================
#### Extract keywords from News data table 
##====================================================================
# Define values to match
#______________________________________________________
filters = []
filters.append({"value": "Machine Learning", "match": ['ML','Machine Learning','MachineLearning','machine learning']})
filters.append({"value":"Deep Learning", "match": ['DL ', 'Deep Learning','DeepLearning','deep learning']})
filters.append({"value": "Big data", "match": ['Big data','bigdata','big data']})
filters.append({"value": "NLP", "match": ['NLP','Natual Language Processing','natural language processing']})
filters.append({"value": "Computer Vision", "match": ['ComputerVision','computer vision', 'Computer Vision']})
filters.append({"value": "AI", "match": ['AI ','Artificial Intelligence', 'ArtificialIntelligence', 'artificial intelligence']})

def match_keywords(text):
    matched=set()
    for f in filters:
        for a in f['match']:
        	match_text = a.decode('utf-8')
            if match_text in text:
                matched.add(f['value'])
    return ','.join(matched)

match_keywords_udf = F.udf(lambda s: match_keywords(s), StringType())
df_news2 = df_news2.withColumn('matched_keywords', match_keywords_udf(df_news2.description))
df_news2.select('matched_keywords').take(5)
df_news2.groupBy('matched_keywords').count().orderBy('count', ascending=False).show()

#### Clean text in News data table 
#______________________________________________________
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer

nltk.download('stopwords')
stopw = stopwords.words('english')
lemmatizer = WordNetLemmatizer()

def process_news(text):
    '''
    Clean news
    '''

    text = text.lower()
    #Tokenize tweets
    tokens = text.split()
    tokens = [t.rstrip() for t in tokens]
    tokens = [t.rstrip(',') for t in tokens]
    tokens = [t.lstrip() for t in tokens]
    tokens = [t for t in tokens if not re.search('\u', t)]
    tokens = [lemmatizer.lemmatize(t) for t in tokens]
    tokens = [t for t in tokens if t not in stopw]
    tokens = [re.sub("[^a-zA-Z]", "", t) for t in tokens]
    filtered_tokens = [t for t in tokens if len(t) <=3 and t not in ['ai','nlp','ml','dl']]
	tokens = [t for t in tokens if t not in filtered_tokens]
    return tokens

clean_text_udf = F.udf(lambda s: process_news(s), ArrayType(StringType()))
df_news2 = df_news2.withColumn('words_desc', clean_text_udf(df_news2.description))

##====================================================================
#### Store cleaned data into Hive tables 
##====================================================================
df_news2.write.saveAsTable('mynewsdb.newsdata')

df_twitter2.write.saveAsTable('mynewsdb.twitterdata')

spark.sql("describe formatted mynewsdb.newsdata").show(truncate = False)
spark.sql("describe formatted mynewsdb.twitterdata").show(truncate = False)








