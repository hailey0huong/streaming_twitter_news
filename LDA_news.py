'''
Try Latent Dirichlet Allocation (LDA) on short description of
each news article from News API

PySpark 2.0

'''

#Import data stored in Hive
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

df_news2 = spark.sql("SELECT * FROM mynewsdb.newsdata")

# CountVectorizer
from pyspark.ml.feature import CountVectorizer , IDF

df_newstext = df_news2.select("words_desc")
df_newstext = df_newstext.withColumn('docID',F.monotonically_increasing_id())

	# fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="words_desc", outputCol="raw_features", vocabSize=3000, minDF=5.0)
cvmodel = cv.fit(df_newstext)

result_cv = cvmodel.transform(df_newstext)
result_cv.show(1,truncate=False)


idf = IDF(inputCol = "raw_features", outputCol = "features")
idfModel = idf.fit(result_cv)
result_tfidf = idfModel.transform(result_cv)


# LDA the whole corpus 
from pyspark.ml.clustering import LDA
num_topics = 10
max_iterations = 100

lda = LDA(k=num_topics, maxIter=max_iterations)
model_lda = lda.fit(result_tfidf)

wordNumbers = 5 
topics = model_lda.describeTopics(maxTermsPerTopic = wordNumbers)

#Print out results
for i in range(num_topics):
    print 'Topic number: ' + str(i)
    words = topics.select('termIndices').collect()[i][0]
    weights = topics.select('termWeights').collect()[i][0]
    topic_words = []
    for n in range(wordNumbers):
        topic_words.append(str(round(weights[n],4))+" * "+cvmodel.vocabulary[words[n]])

    print ' + '.join(topic_words)

