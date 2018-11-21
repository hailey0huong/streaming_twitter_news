'''
This script is to read messages in Kafka streaming, 
process and analyze trend analysis on real-time Tweets
(1) tokenization
(2) stopwords removal
(3) filtering
(4) conversion into Term Document Matrix (TDM)
(5) pattern analysis (TF-IDF)
(6) Visualize real-time data on Plotly

Pyspark 2.0
Python 2.7.15
Kafka 0.8.2

Experiment with hashtags #Bigdata #MachineLearning #AI

'''

batchIntervalSec = 60
windowIntervalSec=600
app_name = 'spark_twitter_topic'
stream_ids = []

username = ''
api_key = ''



#Import dependencies
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import json
import re 
import math
import numpy as np
import plotly.plotly as py  
import plotly.tools as tls   
import plotly.graph_objs as go
from plotly.graph_objs import Scatter, Layout, Figure, Bar
import datetime 
import time
from collections import Counter

py.sign_in(username, api_key)

def unicodetoascii(text):

    TEXT = (text.replace('\\xe2\\x80\\x99', "'").
            replace('\\xc3\\xa9', 'e').
            replace('\\xe2\\x80\\x90', '-').
            replace('\\xe2\\x80\\x91', '-').
            replace('\\xe2\\x80\\x92', '-').
            replace('\\xe2\\x80\\x93', '-').
            replace('\\xe2\\x80\\x94', '-').
            replace('\\xe2\\x80\\x94', '-').
            replace('\\xe2\\x80\\x98', "'").
            replace('\\xe2\\x80\\x9b', "'").
            replace('\\xe2\\x80\\x9c', '"').
            replace('\\xe2\\x80\\x9c', '"').
            replace('\\xe2\\x80\\x9d', '"').
            replace('\\xe2\\x80\\x9e', '"').
            replace('\\xe2\\x80\\x9f', '"').
            replace('\\xe2\\x80\\xa6', '...').
            replace('\\xe2\\x80\\xb2', "'").
            replace('\\xe2\\x80\\xb3', "'").
            replace('\\xe2\\x80\\xb4', "'").
            replace('\\xe2\\x80\\xb5', "'").
            replace('\\xe2\\x80\\xb6', "'").
            replace('\\xe2\\x80\\xb7', "'").
            replace('\\xe2\\x81\\xba', "+").
            replace('\\xe2\\x81\\xbb', "-").
            replace('\\xe2\\x81\\xbc', "=").
            replace('\\xe2\\x81\\xbd', "(").
            replace('\\xe2\\x81\\xbe', ")")

                 )
    return TEXT

stopw = [u'i', u'me', u'my', u'myself', u'we', u'our', 
        u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', 
        u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', 
        u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', 
        u'their', u'theirs', u'themselves', u'what', u'which', u'who', 
        u'whom', u'this', u'that', u'these', u'those', u'am', 
        u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', 
        u'has', u'had', u'having', u'do', u'does', u'did', u'doing', 
        u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', 
        u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', 
        u'about', u'against', u'between', u'into', u'through', u'during', 
        u'before', u'after', u'above', u'below', u'to', u'from', u'up', 
        u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', 
        u'further', u'then', u'once', u'here', u'there', u'when', u'where', 
        u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', 
        u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', 
        u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', 
        u'will', u'just', u'don', u'should', u'now', u'd', u'll', u'm', u'o', 
        u're', u've', u'y', u'ain', u'aren', u'couldn', u'didn', u'doesn', 
        u'hadn', u'hasn', u'haven', u'isn', u'ma', u'mightn', u'mustn', 
        u'needn', u'shan', u'shouldn', u'wasn', u'weren', u'won', u'wouldn', u'would', 
        u'gtgt',u'gtgtgt',u'thank',u'thanks', u'please']

def process_tweet(tweet, hashtag):
    '''
    Clean and filter tweets
    '''

    text = tweet['text'].encode('utf-8')

    #Check if there is hashtag
    if hashtag not in text.lower():
        return (-2,'no searched hashtag',tweet)

    try:
        text = text.replace(r'\n', ' ')
        text = unicodetoascii(text)
        text = re.sub("b\'", '', text)
        text = text.lower()
        #Clean some words in tweets
        text = re.sub(r'machine learning', 'machinelearning', text)
        text = re.sub(r'deep learning', 'deeplearning', text)
        text = re.sub(r'natural language processing', 'naturallanguageprocessing', text)
        text = re.sub(r'artificial intelligence', 'artificialintelligence', text)
        text = re.sub(r'big data', 'bigdata', text)
        text = re.sub(r'computer vision', 'computervision', text)
        #Tokenize tweets
        tokens = text.split()
        tokens = [t for t in tokens if not re.search('http|https', t)]
        tokens = [t for t in tokens if not t.startswith('@')]
        tokens = [t for t in tokens if t != 'rt']
        tokens = [re.sub("[^a-zA-Z]", "", t) for t in tokens]
        tokens = [t.rstrip() for t in tokens]
        tokens = [t.lstrip() for t in tokens]
        tokens = [t for t in tokens if t not in stopw]
        tokens = ['machinelearning' if re.search('^ml$', t) else t for t in tokens ]
        tokens = ['deeplearning'  if re.search('^dl$', t) else t for t in tokens]
        tokens = ['naturallanguageprocessing'  if re.search('^nlp$', t) else t for t in tokens]
        tokens = ['artificialintelligence'  if re.search('^ai$', t) else t for t in tokens]
        tokens = [t for t in tokens if len(t)>3 ]


        tweet['processed_text'] = " ".join( tokens )
    except Exception, err:
        return (-100,err,tweet)

    return (1,tweet)


def collect_text_tweet(tweet):
    return tweet['processed_text']

def count_words(tweet):
    #Return total number of words in each tweet
    tokens = tweet.split(' ')
    return len(tokens)


def create_doc_id(tweetstream):
    n_tweets = len(tweetstream)
    doc_info = []
    i = 0
    for tweet in tweetstream:
        i+=1
        count = count_words(tweet)
        temp = {'id': i, 'tweet_length': count}
        doc_info.append(temp)
    return doc_info



def create_freq_dict(tweetstream):
    #Create a frequency dict for each word in a batch
    i = 0
    freqDict_list = []
    for tweet in tweetstream:
        i+=1
        freq_dict = {}
        tokens = tweet.split(' ')
        for token in tokens:
            if token in freq_dict:
                freq_dict[token] +=1
            else:
                freq_dict[token] = 1
        temp = {'id': i, 'freq_dict': freq_dict}
        freqDict_list.append(temp)
    return freqDict_list

def create_TDM(tweetstream):
    tokens_tweetstream = [t for tweet in tweetstream for t in tweet.split(' ') ]
    TDM = Counter(tokens_tweetstream)
    return TDM

def computeTF(doc_info, freqDict_list):
    '''
    tf = freq of the term in a doc / total number of terms in the doc
    '''
    TF_scores = []
    for tempDict in freqDict_list:
        t_id = tempDict['id']
        for k in tempDict['freq_dict']:
            temp = {'id': t_id,
                    'TF_score': float(tempDict['freq_dict'][k])/float(doc_info[t_id-1]['tweet_length']),
                    'key': k}
            TF_scores.append(temp)
    return TF_scores

def computeIDF(doc_info, freqDict_list):
    '''
    idf = log(total number of docs/ number of docs with term in it)
    '''
    IDF_scores = []
    counter = 0
    for d in freqDict_list:
        counter +=1        
        for k in d['freq_dict'].keys():
            count = sum([k in tempDict['freq_dict'] for tempDict in freqDict_list])
            temp = {'id': counter, 'IDF_score': np.log10(float(len(doc_info))/float(count)), 'key': k}

            IDF_scores.append(temp)

    return IDF_scores

def computeTFIDF(TF_scores, IDF_scores):
    '''
    tf-idf = tf * idf
    '''
    TFIDF_scores = []
    for j in IDF_scores:
        for i in TF_scores:
            if j['key'] == i['key'] and j['id'] == i['id']:
                temp = {'id': j['id'],
                        'TFIDF_score': j['IDF_score']*i['TF_score'],
                        'key': i['key']}
        TFIDF_scores.append(temp)
    return TFIDF_scores


def get_top_tfidf(tweetstream, top_n = 5):    
    #Compute TF-IDF scores for the whole corpus
    doc_info = create_doc_id(tweetstream)
    freqDict_list = create_freq_dict(tweetstream)
    TF_scores = computeTF(doc_info, freqDict_list)
    IDF_scores = computeIDF(doc_info, freqDict_list)
    TFIDF_scores = computeTFIDF(TF_scores, IDF_scores)
    
    #Create term document matrix
    TDM = create_TDM(tweetstream)
    #Get the top 25 keywords in the term document matrix
    top25 = TDM.most_common()[:25] 
    top25_k = [k[0] for k in top25]
    
    #Get TFIDF scores for top 25 keywords
    top25_dict = {}
    for item in top25_k:
        score = [d['TFIDF_score'] for d in TFIDF_scores if d['key']==item ]
        top25_dict[item] = score

    #Filter duplicated keywords:
    tfidf = []
    keyw = []
    for item in top25_dict.keys():
        keyw.append(item)
        tfidf.append(max(top25_dict[item]))
    
    #Get top 5 tfidf scores and keywords
    indices = np.argsort(tfidf)[::-1]
    top_feature_name = [keyw[i] for i in indices[:top_n]]
    top_feature_idf = [tfidf[i] for i in indices[:top_n]]
    top_topics = []
    for i in range(len(top_feature_name)):
        top_topics.append((top_feature_name[i], top_feature_idf[i]))

    return top_topics, top_feature_name, top_feature_idf
    


def visualize_data_plotly(top_feature_name, top_feature_idf, topic, stream_id ):
    # Make instance of stream id object 
    trace1 = Scatter(
                    x=[],
                    y=[],
                    text = [],
                    mode='markers+text',
                    textposition='bottom center',
                    stream=dict(
                        token=stream_id,
                        maxpoints=100
                            )
                    )

    layout = Layout(title='Twitter Streaming Keywords by '+topic+' Topic',
                    xaxis = {'title': 'current time'},
                    yaxis={'title':'tf-idf score', 'range':[0,1]} 
                    )
    fig = Figure(data=[trace1], layout=layout)

    print py.plot(fig, filename='Twitter Streaming Keywords by '+topic)

    #Set up stream link object
    s = py.Stream(stream_id)
    s.open()

    time.sleep(5)
    
    for i in range(len(top_feature_name)):    
        # Current time on x-axis, tfidf score on the y-axis
        x = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        y = top_feature_idf[i]
        text = top_feature_name[i]
            # Send data to your plot
        s.write({'x':x, 'y': y, 'text': text })  
                
     
        time.sleep(0.5)  # plot a point every 0.5 second  


    #s.close()   


def get_viz_bigdata(rdd):
    tweetstream = rdd.collect()
    top_topics, top_feature_name, top_feature_idf = get_top_tfidf(tweetstream)
    visualize_data_plotly(top_feature_name, top_feature_idf, 'Big data', stream_ids[0])
    print "__________________________"
    print "Topics in Big data"
    print "__________________________"
    for t in top_topics:
        print t[0] + ": "+"{0:0.6f}".format(t[1])
    print "__________________________"

def get_viz_AI(rdd):
    tweetstream = rdd.collect()
    top_topics, top_feature_name, top_feature_idf = get_top_tfidf(tweetstream)
    visualize_data_plotly(top_feature_name, top_feature_idf, 'AI', stream_ids[0])
    print "__________________________"
    print "Topics in AI"
    print "__________________________"
    for t in top_topics:
        print t[0] + ": "+"{0:0.6f}".format(t[1])
    print "__________________________"
    

def get_viz_ML(rdd):
    tweetstream = rdd.collect()
    top_topics, top_feature_name, top_feature_idf = get_top_tfidf(tweetstream)
    visualize_data_plotly(top_feature_name, top_feature_idf, 'Machine Learning', stream_ids[2])
    print "__________________________"
    print "Topics in Machine Learning"
    print "__________________________"
    for t in top_topics:
        print t[0] + ": "+"{0:0.6f}".format(t[1])
    print "__________________________"

## Function: send messages to Kafka
    # create topics for notmached and error to review and debug codes if neccessary.
def send_to_kafka_matched(partition):
    kafka_prod = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
    for record in partition:
        kafka_prod.send('twitter_matched_tdidf', str(json.dumps(record)))

def send_to_kafka_not_matched(partition):
    kafka_prod = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
    for record in partition:
        kafka_prod.send('twitter_processed_notmatched', str(json.dumps(record)))


#Function: Streaming context definition
def createContext():
    sc = SparkContext(appName=app_name)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, batchIntervalSec)
    

    # Define Kafka Consumer and Producer
    kafkaStream = KafkaUtils.createStream(ssc, 'sandbox-hdp.hortonworks.com:2181', app_name, {'twitter_streams':1})
    
    # Get the JSON tweets payload
    tweets_dstream = kafkaStream.map(lambda v: json.loads(v[1]))

    ## Inbound Tweet counts
    inbound_batch_cnt = tweets_dstream.count()
    inbound_window_cnt = tweets_dstream.countByWindow(windowIntervalSec,batchIntervalSec)

    ## Process Batch Streams
    ## _________________________________________________
    ## Match tweet to trigger criteria
    tweets_bigdata = tweets_dstream.map(lambda tweet: process_tweet(tweet, hashtag = '#bigdata'))
    matched_tweets_bigdata = tweets_bigdata.filter(lambda processed_tweet: processed_tweet[0]>0).map(lambda processed_tweet: processed_tweet[1])

    tweets_ml = tweets_dstream.map(lambda tweet: process_tweet(tweet, hashtag = '#machinelearning'))
    matched_tweets_ml = tweets_ml.filter(lambda processed_tweet: processed_tweet[0]>0).map(lambda processed_tweet: processed_tweet[1])

    tweets_ai = tweets_dstream.map(lambda tweet: process_tweet(tweet, hashtag = '#ai'))
    matched_tweets_ai = tweets_ai.filter(lambda processed_tweet: processed_tweet[0]>0).map(lambda processed_tweet: processed_tweet[1])

    ## Send the matched data to Kafka topic
    matched_tweets_bigdata.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_matched))
    matched_tweets_ml.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_matched))
    matched_tweets_ai.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_matched))

    bigdata_matched_batch_cnt = matched_tweets_bigdata.count()
    ml_matched_batch_cnt = matched_tweets_ml.count()
    ai_matched_batch_cnt = matched_tweets_ai.count()
    bigdata_matched_window_cnt = matched_tweets_bigdata.countByWindow(windowIntervalSec,batchIntervalSec)
    ml_matched_window_cnt = matched_tweets_ml.countByWindow(windowIntervalSec,batchIntervalSec)
    ai_matched_window_cnt = matched_tweets_ai.countByWindow(windowIntervalSec,batchIntervalSec)
    
    ##  Check if there is any error in processing tweets (for debugging purpose)

    errored_tweets_bigdata = tweets_bigdata.filter(lambda processed_tweet:(processed_tweet[0]<=-100))
    errored_tweets_ml = tweets_ml.filter(lambda processed_tweet:(processed_tweet[0]<=-100))
    errored_tweets_ai = tweets_ai.filter(lambda processed_tweet:(processed_tweet[0]<=-100))

    bigdata_errored_batch_cnt = errored_tweets_bigdata.count()
    bigdata_errored_window_cnt = errored_tweets_bigdata.countByWindow(windowIntervalSec,batchIntervalSec)

    
    ## Print batch counts
    inbound_batch_cnt.map(lambda x:('Batch/Inbound: %s' % x))\
                    .union(bigdata_matched_batch_cnt.map(lambda x:('Batch/Bigdata: %s' % x))\
                    .union(ml_matched_batch_cnt.map(lambda x: ('Batch/ML: %s' % x))\
                    .union(ai_matched_batch_cnt.map(lambda x:('Batch/AI: %s' % x))\
                    .union(bigdata_errored_batch_cnt.map(lambda x:('Batch/Errored/Bigdata: %s' % x)))))).pprint()
    
    ## Process Window Streams
    ## _______________________________
    bigdata_matched_window = matched_tweets_bigdata.window(windowIntervalSec,batchIntervalSec)
    ml_matched_window = matched_tweets_ml.window(windowIntervalSec,batchIntervalSec)
    ai_matched_window = matched_tweets_ai.window(windowIntervalSec,batchIntervalSec)

    ## Print counts
    inbound_window_cnt.map(lambda x:('Window/Inbound: %s' % x))\
                    .union(bigdata_matched_window_cnt.map(lambda x:('Window/Bigdata: %s' % x))\
                    .union(ml_matched_window_cnt.map(lambda x:('Window/ML: %s' % x))\
                    .union(ai_matched_window_cnt.map(lambda x:('Window/AI: %s' % x))\
                    .union(bigdata_errored_window_cnt.map(lambda x: ('Window/Error/Bigdata: %s' % x)))))).pprint()

    ## TF-IDF tweets
    streams_ai = ai_matched_window.map(lambda tweet: collect_text_tweet(tweet))
    topic_ai = streams_ai.foreachRDD(get_viz_AI)

    streams_bigdata = bigdata_matched_window.map(lambda tweet: collect_text_tweet(tweet))
    topic_bigdata = streams_bigdata.foreachRDD(get_viz_bigdata)

    streams_ml = ml_matched_window.map(lambda tweet: collect_text_tweet(tweet))
    topic_ml = streams_ml.foreachRDD(get_viz_ML)

    return ssc

     

if __name__=="__main__":
	ssc = StreamingContext.getOrCreate('/tmp/%s' % app_name,lambda: createContext())
	ssc.start()
	ssc.awaitTermination(timeout = 3000)

