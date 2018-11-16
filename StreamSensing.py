'''
This script is to read messages in Kafka streaming, 
process and analyze trend analysis on real-time Tweets
(1) tokenization
(2) stopwords removal
(3) filtering
(4) conversion into TF-IDF
(5) pattern analysis

Pyspark 2.0
Python 2.7.15
Kafka 0.8.2

Experiment with hashtags #Bigdata #MachineLearning #AI

'''

batchIntervalSec = 300
windowIntervalSec=1500
app_name = 'spark_twitter_topic_24'

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
        u'needn', u'shan', u'shouldn', u'wasn', u'weren', u'won', u'wouldn']

def process_tweet(tweet, hashtag):
    '''
    Clean and filter tweets
    '''

    text = tweet['text'].encode('utf-8').replace('\n', ' ')

    #Check if there is hashtag
    if hashtag not in text.lower():
        return (-2,'no searched hashtag',tweet)

    try:
        #Remove hyperlink
        text = re.sub('https+.*', '',text)
        #Clean special characters:
        text = unicodetoascii(text)
        #Exclude mentions
        text = re.sub('@\w+','', text)
        # Remove non-letters        
        letters_only = re.sub("[^a-zA-Z]", " ", text)

        tweet['cleaned_text'] = letters_only 
    except Exception, err:
        return (-100,err,tweet)
    
    try:
        
        cleaned_text = tweet['cleaned_text']
        tokens = cleaned_text.lower().split()
        #Remove RT symbol
        tokens = [t for t in tokens if t != 'rt']
        #tokens = [lemmatizer.lemmatize(t) for t in tokens]
        tokens = [t for t in tokens if t not in stopw]
        tokens = [t for t in tokens if len(t)>3]
        tweet['processed_text'] = " ".join( tokens )
    except Exception, err:
        return (-101, err, tweet)


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

def computeTF(doc_info, freqDict_list):
    '''
    tf = freq of the term in a doc / total number of terms in the doc
    '''
    TF_scores = []
    for tempDict in freqDict_list:
        t_id = tempDict['id']
        for k in tempDict['freq_dict']:
            temp = {'id': t_id,
                    'TF_score': tempDict['freq_dict'][k]/doc_info[t_id-1]['tweet_length'],
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
            temp = {'id': counter, 'IDF_score': np.log10(len(doc_info)/count), 'key': k}

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

def get_top_tfidf(rdd):
    tweetstream = rdd.collect()
    top_n = 5
    doc_info = create_doc_id(tweetstream)
    freqDict_list = create_freq_dict(tweetstream)
    TF_scores = computeTF(doc_info, freqDict_list)
    IDF_scores = computeIDF(doc_info, freqDict_list)
    TFIDF_scores = computeTFIDF(TF_scores, IDF_scores)
    tfidf = []
    key = []
    for score in TFIDF_scores:
        tfidf.append(score['TFIDF_score'])
        key.append(score['key'])
    indices = np.argsort(tfidf)[::-1]
    top_feature_name = [key[i] for i in indices[:top_n]]
    top_feautre_idf = [tfidf[i] for i in indices[:top_n]]
    top_topics = []
    for i in range(len(top_feature_name)):
        top_topics.append((top_feature_name[i], top_feautre_idf[i]))

    for t in top_topics:
        print t[0] + ": "+"{0:0.10f}".format(t[1])





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
    
    ##  Print any erroring tweets

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
    streams_bigdata = bigdata_matched_window.map(lambda tweet: collect_text_tweet(tweet))
    topic_bigdata = streams_bigdata.foreachRDD(get_top_tfidf)

    streams_ml = ml_matched_window.map(lambda tweet: collect_text_tweet(tweet))
    topic_ml = streams_ml.foreachRDD(get_top_tfidf)

    streams_ai = ai_matched_window.map(lambda tweet: collect_text_tweet(tweet))
    topic_ai = streams_ai.foreachRDD(get_top_tfidf)



    return ssc

     

if __name__=="__main__":
	ssc = StreamingContext.getOrCreate('/tmp/%s' % app_name,lambda: createContext())
	ssc.start()
	ssc.awaitTermination(timeout = 3000)

