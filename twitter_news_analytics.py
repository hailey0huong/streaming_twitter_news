'''
This script includes some analysis on Spark:
1. Analyze News data
2. Analyze Twitter data
3. Merge News data and Twitter data
4. Visualize on Plotly

'''

##==================================================================
#### Connect to Hive:
##==================================================================
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

## Get data from Hive
df_news2 = spark.sql("SELECT * FROM mynewsdb.newsdata")
df_twitter2 = spark.sql("SELECT * FROM mynewsdb.twitterdata")


##==================================================================
#### News data
##==================================================================

source_data = df_news2.groupBy('PublishedDate','source_name').count().orderBy(['PublishedDate','count'], ascending=[0,0])

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number
import pyspark.sql.functions as F

window_source = Window.partitionBy(source_data['PublishedDate']).orderBy(source_data['count'].desc())

source_data_top3 = source_data.select('*', row_number().over(window_source).alias('row_number')).where(col('row_number') <= 3)
source_data_top3 = source_data_top3.orderBy(['PublishedDate','count'], ascending = [0,0])


# Top source_name for topics:
top_source_data = df_news2.groupBy('source_name').count().orderBy(['count'], ascending=[0])
total_publication = df_news2.count()
top_source_data = top_source_data.withColumn("relative_rank", F.col('count')/total_publication)
top_source_data.show()

# Trends of keywords
news_bykeywords = df_news2.groupBy('PublishedDate','matched_keywords').count().withColumnRenamed("count", "count_matched").orderBy(['PublishedDate','count_matched'], ascending=[0,0])
total_news_byday = df_news2.groupBy('PublishedDate').count().withColumnRenamed("count", "total").orderBy('PublishedDate', ascending = False)
news_bykeywords2 = news_bykeywords.join(total_news_byday, news_bykeywords.PublishedDate == total_news_byday.PublishedDate, how = 'left').drop(total_news_byday.PublishedDate)
news_bykeywords2 = news_bykeywords2.withColumn('relative_count', (F.col('count_matched')/ F.col('total')))
news_bykeywords2 = news_bykeywords2.orderBy(['PublishedDate','relative_count'], ascending = [0,0])
news_bykeywords2.show()

   #Select only timeframe with tweets
df_news3 = df_news2.where((df_news2['PublishedDate'] >= min_date) & (df_news2['PublishedDate'] <= max_date))
df_news3.select(F.countDistinct('PublishedDate')).show()

news_bykeywords2_shorten = news_bykeywords2.where((news_bykeywords2['PublishedDate'] >= min_date) & (news_bykeywords2['PublishedDate'] <= max_date))
news_bykeywords2_shorten.select(F.countDistinct('PublishedDate')).show()


	#Trend by group keywords (does not matter which day, but in the same time range with tweets)
news_bykey = df_news3.groupBy('matched_keywords').count().withColumnRenamed("count", "count_matched").orderBy(['count_matched'], ascending=[0])
total_news2 = df_news3.count()
news_bykey2 = news_bykey.withColumn('relative_count', (F.col('count_matched')/ total_news2))
news_bykey2 = news_bykey2.filter(news_bykey2['matched_keywords']!= "")
news_bykey2.show()


##==================================================================
#### Twitter Data
##==================================================================
min_date, max_date = df_twitter2.select(F.min("created_at_dt"), F.max("created_at_dt")).first()

# Keywords count
tweet_bykeywords = df_twitter2.groupBy('created_at_dt','tweet_matched_words').count().withColumnRenamed("count", "count_matched").orderBy(['created_at_dt','count_matched'], ascending=[0,0])
total_tweets_byday = df_twitter2.groupBy('created_at_dt').count().withColumnRenamed("count", "total").orderBy('created_at_dt', ascending = False)
tweet_bykeywords2 = tweet_bykeywords.join(total_tweets_byday, tweet_bykeywords.created_at_dt == total_tweets_byday.created_at_dt, how = 'left').drop(total_tweets_byday.created_at_dt)
tweet_bykeywords2 = tweet_bykeywords2.withColumn('relative_count', (F.col('count_matched')/ F.col('total')))
tweet_bykeywords2 = tweet_bykeywords2.orderBy(['created_at_dt','relative_count'], ascending = [0,0])


	#Trend by group keywords (does not matter which day)
tweet_bykey = df_twitter2.groupBy('tweet_matched_words').count().withColumnRenamed("count", "count_matched").orderBy(['count_matched'], ascending=[0])
total_tweets2 = df_twitter2.count()
tweet_bykey2 = tweet_bykey.withColumn('relative_count', (F.col('count_matched')/ total_tweets2))
tweet_bykey2.show()


# Top Influencers
df_twitter2.groupBy('screen_name','user_followers_count').count().orderBy(['user_followers_count'], ascending=[0]).show()
influencers = df_twitter2.groupBy('screen_name','user_followers_count').count().withColumnRenamed("count", "count_posting").orderBy(['count_posting','user_followers_count'], ascending=[0,0])



# Top domains
domain_data = df_twitter2.groupBy('created_at_dt','domain').count().orderBy(['created_at_dt','count'], ascending=[0,0])

window_domain = Window.partitionBy(domain_data['created_at_dt']).orderBy(domain_data['count'].desc())

domain_data_top3 = domain_data.select('*', row_number().over(window_domain).alias('row_number')).where(col('row_number') <= 3)
domain_data_top3 = domain_data_top3.orderBy(['created_at_dt','count'], ascending = [0,0])

top_domain_data = df_twitter2.groupBy('domain').count().orderBy(['count'], ascending=[0])
total_tweets = df_twitter2.count()
top_domain_data = top_domain_data.withColumn("relative_rank", F.col('count')/total_tweets)
top_domain_data = top_domain_data.orderBy('relative_rank', ascending = False)
top_domain_data.show()

df_twitter2.filter(df_twitter2['domain'] =='co.uk').select('url').take(5)


##==================================================================
#### Merge News and Twitter data
##==================================================================
## Merge by URL
inner_join_news = df_twitter2.join(df_news2, df_twitter2.url == df_news2.url).drop(df_twitter2.url)
inner_join_news.select(F.countDistinct('url')).show()
inner_join_news.count()


news_join = inner_join_news.select('url','text','content','description','words_desc')
news_join.take(1)

##Combine text, content, description
news_join = news_join.withColumn('all_content',F.concat_ws(' ', news_join.text, news_join.content, news_join.description))
news_join_distinct = news_join.dropDuplicates(['url']) #get distinct news
shared_news_data = [i.all_content for i in news_join_distinct.select('all_content').collect()]

#Clean text
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer

nltk.download('stopwords')
stopw = stopwords.words('english')
lemmatizer = WordNetLemmatizer()


def process_text(text):
	text = text.replace(r'\n', ' ')
	text = re.sub("b\'", '', text)
	text = text.lower()
    tokens = text.split()
    tokens = [t.rstrip() for t in tokens]
    tokens = [t.rstrip(',') for t in tokens]
    tokens = [t.lstrip() for t in tokens]
    tokens = [t for t in tokens if not re.search('\u', t)]
    tokens = [t for t in tokens if not re.search('http|https', t)]
    tokens = [t for t in tokens if t != 'rt']
    tokens = [lemmatizer.lemmatize(t) for t in tokens]
    tokens = [t for t in tokens if t not in stopw]
    tokens = [re.sub("[^a-zA-Z]", "", t) for t in tokens]
    filtered_tokens = [t for t in tokens if len(t) <=3 and t not in ['ai','nlp','ml','dl']]
	tokens = [t for t in tokens if t not in filtered_tokens]
    return ' '.join(tokens)

#Create TDM for the news
shared_news_data = [process_text(t) for t in shared_news_data]

from collections import Counter
def create_TDM(data):
    tokens_news = [t for news in data for t in news.split(' ') ]
    TDM = Counter(tokens_news)
    return TDM

TDM = create_TDM(shared_news_data)
top25 = TDM.most_common()[:25] 

#Save to textfile to visualize wordcloud
sc.parallelize([TDM]).coalesce(1, shuffle = True).saveAsTextFile('/user/maria_dev/TDM2')
# hadoop fs -get /user/maria_dev/TDM2/part-00000 /home/maria_dev



##==================================================================
#### Ploting on Plotly
##==================================================================
username = ''
api_key = ''
import plotly.plotly as py  
import plotly.tools as tls   
import plotly.graph_objs as go
from plotly.graph_objs import Scatter, Layout, Figure

py.sign_in(username, api_key)


#--------------------------------------------------------
#Plot number of keywords by day (Twitter)
#--------------------------------------------------------
ML_by_day = tweet_bykeywords2.filter(tweet_bykeywords2['tweet_matched_words']=='Machine Learning')
bigdata_by_day = tweet_bykeywords2.filter(tweet_bykeywords2['tweet_matched_words']=='Big data')
AI_by_day = tweet_bykeywords2.filter(tweet_bykeywords2['tweet_matched_words']=='AI')
DL_by_day = tweet_bykeywords2.filter(tweet_bykeywords2['tweet_matched_words']=='Deep Learning')

ml_day = [i.created_at_dt for i in ML_by_day.select('created_at_dt').collect()]
ml_count = [float(i.relative_count) for i in ML_by_day.select('relative_count').collect()]

bigdata_day = [i.created_at_dt for i in bigdata_by_day.select('created_at_dt').collect()]
bigdata_count = [float(i.relative_count) for i in bigdata_by_day.select('relative_count').collect()]

AI_day = [i.created_at_dt for i in AI_by_day.select('created_at_dt').collect()]
AI_count = [float(i.relative_count) for i in AI_by_day.select('relative_count').collect()]

DL_day = [i.created_at_dt for i in DL_by_day.select('created_at_dt').collect()]
DL_count = [float(i.relative_count) for i in DL_by_day.select('relative_count').collect()]

trace0 = go.Bar(
    x=ml_day,
    y=ml_count,
    name='Machine Learning',
    marker=dict(
        color='rgb(49,130,189)'
    )
)
trace1 = go.Bar(
    x=bigdata_day,
    y=bigdata_count,
    name='Big data',
    marker=dict(
        color='rgb(204,204,204)',
    )
)

trace2 = go.Bar(
    x=AI_day,
    y=AI_count,
    name='AI',
    marker=dict(
        color='rgb(158,202,225)',
    )
)

trace3 = go.Bar(
    x=DL_day,
    y=DL_count,
    name='Deep Learning',
    marker=dict(
        color='rgb(58,96,183)',
    )
)


data = [trace0, trace1, trace2, trace3]
layout = go.Layout(title='Keywords on Tweets by Day',
    xaxis=dict(tickangle=-45),
    yaxis={'title':'relative count'},
    barmode='group',
)

fig = go.Figure(data=data, layout=layout)
py.plot(fig, filename='twitter_keywords')


#--------------------------------------------------------
#Plot influencers (Twitter)
#--------------------------------------------------------
import numpy as np
influencers_fcount = [float(i.user_followers_count) for i in influencers.select('user_followers_count').collect()[:20]]
influencers_pcount = [int(i.count_posting) for i in influencers.select('count_posting').collect()[:20]]
influencers_users = [i.screen_name for i in influencers.select('screen_name').collect()[:20]]
x_value = list(np.arange(1,len(influencers_users)*2,2))
size = [float(i/1000)for i in influencers_fcount]

trace4 = go.Scatter(
					x = x_value,
					y = influencers_pcount,
					text = influencers_users,
					mode = 'markers+text',
					marker = dict(color = influencers_fcount,
									size = size,
									showscale = True,
									colorbar=dict(
                					title='follower count')
                					)
	)


layout2 = go.Layout(title='Twitter Influencers',
                    yaxis={'title':'posting count'} 
                    )

fig2 = go.Figure(data=[trace4], layout=layout2)
py.plot(fig2, filename='twitter_influencers')


#--------------------------------------------------------
#Plot Domains (Twitter)
#--------------------------------------------------------
top_domain_rank = [float(i.relative_rank) for i in top_domain_data.select('relative_rank').collect()[:15]]
top_domain_name = [i.domain for i in top_domain_data.select('domain').collect()[:15]]

trace5 = go.Bar(
    x=top_domain_rank,
    y=top_domain_name,
    marker=dict(
        color='rgba(50, 171, 96, 0.6)',
        line=dict(
            color='rgba(50, 171, 96, 1.0)',
            width=1),
    ),
    orientation='h',
)

layout3 = go.Layout(title='Twitter Top Shared Domains',
					xaxis = dict(title = 'relative count of sharing'),
                    yaxis=dict(autorange="reversed",automargin=True))

fig3 = go.Figure(data=[trace5], layout=layout3)
py.plot(fig3, filename='twitter_domain')

#--------------------------------------------------------
#Plot keywords rank (Twitter)
#--------------------------------------------------------
top_key_rank = [float(i.relative_count) for i in tweet_bykey2.select('relative_count').collect()]
top_key_name = [i.tweet_matched_words for i in tweet_bykey2.select('tweet_matched_words').collect()]

trace7 = go.Bar(
    x=top_key_rank,
    y=top_key_name,
    marker=dict(
        color='rgba(219, 64, 82, 0.7)',
        line=dict(
            color='rgba(219, 64, 82, 1.0)',
            width=2),
    ),
    orientation='h',
)

layout5 = go.Layout(title='Twitter Top Keywords',
					xaxis = dict(title = 'relative count'),
                    yaxis=dict(autorange="reversed",automargin=True))

fig5 = go.Figure(data=[trace7], layout=layout5)
py.plot(fig5, filename='twitter_keywords_all')


#--------------------------------------------------------
#Plot Domains (News)
#--------------------------------------------------------
top_source_rank = [float(i.relative_rank) for i in top_source_data.select('relative_rank').collect()[:15]]
top_source_name = [i.source_name for i in top_source_data.select('source_name').collect()[:15]]

trace6 = go.Bar(
    x=top_source_rank,
    y=top_source_name,
    marker=dict(
        color='rgba(50, 171, 96, 0.6)',
        line=dict(
            color='rgba(50, 171, 96, 1.0)',
            width=1),
    ),
    orientation='h',
)

layout4 = go.Layout(title='Top Sources for Relevant News',
					xaxis = dict(title = 'relative count of published relevant articles'),
                    yaxis=dict(autorange="reversed",automargin=True))

fig4 = go.Figure(data=[trace6], layout=layout4)
py.plot(fig4, filename='news_domain')

#--------------------------------------------------------
#Plot keywords by day (News)
#--------------------------------------------------------
ML_by_day1 = news_bykeywords2_shorten.filter(news_bykeywords2_shorten['matched_keywords']=='Machine Learning')
bigdata_by_day1 = news_bykeywords2_shorten.filter(news_bykeywords2_shorten['matched_keywords']=='Big data')
AI_by_day1 = news_bykeywords2_shorten.filter(news_bykeywords2_shorten['matched_keywords']=='AI')
DL_by_day1 = news_bykeywords2_shorten.filter(news_bykeywords2_shorten['matched_keywords']=='Deep Learning')

ml_day1 = [i.PublishedDate for i in ML_by_day1.select('PublishedDate').collect()]
ml_count1 = [float(i.relative_count) for i in ML_by_day1.select('relative_count').collect()]

AI_day1 = [i.PublishedDate for i in AI_by_day1.select('PublishedDate').collect()]
AI_count1 = [float(i.relative_count) for i in AI_by_day1.select('relative_count').collect()]

DL_day1 = [i.PublishedDate for i in DL_by_day1.select('PublishedDate').collect()]
DL_count1 = [float(i.relative_count) for i in DL_by_day1.select('relative_count').collect()]

bigdata_day1 = [i.PublishedDate for i in bigdata_by_day1.select('PublishedDate').collect()]
bigdata_count1 = [float(i.relative_count) for i in bigdata_by_day1.select('relative_count').collect()]

trace0 = go.Bar(
    x=ml_day1,
    y=ml_count1,
    name='Machine Learning',
    marker=dict(
        color='rgb(49,130,189)'
    )
)

trace1 = go.Bar(
    x=bigdata_day1,
    y=bigdata_count1,
    name='Big data',
    marker=dict(
        color='rgb(204,204,204)',
    )
)


trace2 = go.Bar(
    x=AI_day1,
    y=AI_count1,
    name='AI',
    marker=dict(
        color='rgb(158,202,225)',
    )
)

trace3 = go.Bar(
    x=DL_day1,
    y=DL_count1,
    name='Deep Learning',
    marker=dict(
        color='rgb(58,96,183)',
    )
)


data = [trace0,trace1, trace2, trace3]

layout = go.Layout(title='Keywords on Articles by Day',
    xaxis=dict(tickangle=-45),
    yaxis={'title':'relative count'},
    barmode='group',
)

fig = go.Figure(data=data, layout=layout)
py.plot(fig, filename='news_keywords')

#--------------------------------------------------------
#Plot keywords rank (News)
#--------------------------------------------------------
top_key_rank1 = [float(i.relative_count) for i in news_bykey2.select('relative_count').collect()]
top_key_name1 = [i.matched_keywords for i in news_bykey2.select('matched_keywords').collect()]

trace7 = go.Bar(
    x=top_key_rank1,
    y=top_key_name1,
    marker=dict(
        color='rgba(219, 64, 82, 0.7)',
        line=dict(
            color='rgba(219, 64, 82, 1.0)',
            width=2),
    ),
    orientation='h',
)

layout5 = go.Layout(title='Top Keywords in Articles',
					xaxis = dict(title = 'relative count'),
                    yaxis=dict(autorange="reversed",automargin=True))

fig5 = go.Figure(data=[trace7], layout=layout5)
py.plot(fig5, filename='news_keywords_all')


##==================================================================

