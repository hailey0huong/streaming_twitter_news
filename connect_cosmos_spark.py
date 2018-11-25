'''
This script is to connect Spark to Cosmos DB 
and retrieve the data in Spark
'''

import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import datetime


### Set up Cosmos DB connection
host = ""
masterKey = ""

	# Configure Database and Collections

databaseId = 'newsdb'
collectionId_1 = 'newsapi_data'
collectionId_2 = 'twitterapi_data'

connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)


	# Configurations the Azure Cosmos DB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink_1 = dbLink + '/colls/' + collectionId_1
collLink_2 = dbLink + '/colls/' + collectionId_2

	# Set query parameter
querystr1 = "SELECT * FROM newsapi_data"
querystr2 = "SELECT * FROM twitterapi_data"


# Query documents
query1 = client.QueryDocuments(collLink_1, querystr1, options=None, partition_key=None)
query2 = client.QueryDocuments(collLink_2, querystr2, options=None, partition_key=None)

# Create `df` Spark DataFrame from `elements` Python list
	#To convert News API data from query to DF
from pyspark.sql.types import *
data_schema1 = [StructField('author', StringType(), True),
				StructField('content', StringType(), True),
				StructField('description', StringType(), True),
				StructField('publishedAt', StringType(), True),
				StructField('source_name', StringType(), True),
				StructField('title', StringType(), True),
				StructField('url', StringType(), True)]
df_news = spark.createDataFrame(list(query1), StructType(fields = data_schema1))

	#To convert twitter data query to DF 

data_schema = [StructField('domain', StringType(), True),
				StructField('tweet_match_count', IntegerType(), True),
				StructField('user_followers_count', IntegerType(), True),
				StructField('text', StringType(), True),
				StructField('mention', StringType(), True),
				StructField('user_loc', StringType(), True),
				StructField('tweet_geo', StringType(), True),
				StructField('screen_name', StringType(), True),
				StructField('url', StringType(), True),
				StructField('created_at', StringType(), True),
				StructField('user_time_zone', StringType(), True),
				StructField('twitter_RT', StringType(), True),
				StructField('tweet_favorite_count', IntegerType(), True),
				StructField('retweet_status', IntegerType(), True),
				StructField('tweet_place', StringType(), True),
				StructField('tweet_coordinates', StringType(), True),
				StructField('tweet_matched_words', StringType(), True),
				StructField('user_name', StringType(), True),
				StructField('tweet_retweet_count', IntegerType(), True)]

final_struc = StructType(fields = data_schema)
df_twitter = spark.createDataFrame(list(query2), final_struc)

df_twitter.count()
df_news.count()





