'''
This script is to read messages in Kafka streaming, 
process and send matched tweets to a new Kafka topic.

Added Text processing and connection to store data in Cosmos DB

Pyspark 2.0
Python 2.7.15
Kafka 0.8.2
'''

batchIntervalSec = 600
windowIntervalSec=1800 
app_name = 'spark_twitter'

#Import dependencies
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import json
import re


#Define values to match
filters = []
filters.append({"value": "Machine Learning", "match": ['ML','Machine Learning','MachineLearning','machine learning']})
filters.append({"value":"Deep Learning", "match": ['DL ', 'Deep Learning','DeepLearning','deep learning']})
filters.append({"value": "Big data", "match": ['Big data','bigdata','big data']})
filters.append({"value": "NLP", "match": ['NLP','Natual Language Processing','natural language processing']})
filters.append({"value": "Computer Vision", "match": ['ComputerVision','computer vision', 'Computer Vision']})
filters.append({"value": "AI", "match": ['AI ','Artificial Intelligence', 'ArtificialIntelligence', 'artificial intelligence']})



#define whitelisted domains
domain_whitelist=[]
domain_whitelist.append("wikipedia.org")
domain_whitelist.append("twitter.com")


#Function: Unshorten shortened URLs
import httplib
import urlparse

def unshorten_url(url):
    parsed = urlparse.urlparse(url)
    h = httplib.HTTPConnection(parsed.netloc)
    h.request('HEAD', parsed.path)
    response = h.getresponse()
    if response.status/100 == 3 and response.getheader('Location'):
        return response.getheader('Location')
    else:
        return url

#Function: get user mention in tweet
def get_mention(text):
    ref = re.findall('(?<=@)\w+', text)
    return ref

#Function: clean some special characters in tweets 
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


#Function: process tweets
def process_tweet(tweet):
    if tweet.get('retweeted_status'):
        tweet['retweet_status'] = 1
    else:
        tweet['retweet_status'] = 0

    # Check that there's a URLs in the tweet 
    if not tweet.get('entities'):
        return (-2,'no entities element',tweet)

    if not tweet.get('entities').get('urls'):
        return (-3,'no entities.urls element',tweet)

    # Collect all the domains linked to in the tweet
    url_info={}
    url_info['domain']=[]
    url_info['primary_domain']=[]
    url_info['full_url']=[]
    try:
        for url in tweet['entities']['urls']:
            try:
                expanded_url = url['expanded_url']
            except Exception, err:
                return (-104,err,tweet)
            
            # Try to resolve the URL
            try:
                expanded_url = unshorten_url(expanded_url)
            except Exception, err:
                return (-108,err,tweet)
            
            # Determine the domain
            try:
                domain = urlparse.urlsplit(expanded_url).netloc
            except Exception, err:
                return (-107,err,tweet)
            try:
                # Extract the 'primary' domain
                re_result = re.search('(\w+\.\w+)$',domain)
                if re_result:
                    primary_domain = re_result.group(0)
                else:
                    primary_domain = domain
            except Exception, err:
                return (-105,err,tweet)

            try:
                url_info['domain'].append(domain)
                url_info['primary_domain'].append(primary_domain)
                url_info['full_url'].append(expanded_url)
            except Exception, err:
                return (-106,err,tweet)

            
        # Check domains against the whitelist
        # If every domain found is in the whitelist, we can ignore them
        try:
            if set(url_info['primary_domain']).issubset(domain_whitelist):
                return (-8,'All domains whitelisted',tweet)
        except Exception, err:
            return (-103,err,tweet)
            

    except Exception, err:
        return (-102,err,tweet)
    
    # Parse the tweet text against list of trigger terms
    matched=set()
    try:
    	tweet_text = tweet['text']
        for f in filters:
        	for a in f['match']:
        		match_text = a.decode('utf-8')
                if match_text in tweet_text:
                	matched.add(f['value'])
    except Exception, err:
        return (-101,err,tweet)
    
    # Add the discovered metadata into the tweet object that this function will return
    try:
        tweet['enriched']={}
        tweet['enriched']['matched_words']=list(matched)
        tweet['enriched']['url_details']=url_info
        tweet['enriched']['match_count']=len(matched)

    except Exception, err:
        return (-100,err,tweet)
            
    return (len(matched),tweet)

def process_matchedtweet(tweet):
    '''
    to read json file and only select important data
    input: json
    output: a dictionary of important info in tweet
    '''
    tweet_important_info = {}
    tweet_important_info['id'] = str(tweet['id'])
    tweet_important_info['retweet_status'] = tweet['retweet_status']
    tweet_important_info['created_at'] = tweet['created_at']
    tweet_important_info['text'] = unicodetoascii(tweet['text'].encode('utf-8'))
    tweet_important_info['tweet_match_count'] = tweet['enriched']['match_count']
    tweet_important_info['tweet_matched_words'] = ','.join(tweet['enriched']['matched_words'])
    tweet_important_info['user_name'] = tweet['user']['name']
    tweet_important_info['screen_name'] = tweet['user']['screen_name']
    tweet_important_info['user_followers_count'] = tweet['user']['followers_count']
    tweet_important_info['url'] = ','.join(tweet['enriched']['url_details']['full_url'])
    tweet_important_info['user_time_zone'] = tweet['user']['time_zone']
    tweet_important_info['user_loc'] = tweet['user']['location']
    tweet_important_info['mention'] = ','.join(get_mention(tweet['text']))
    tweet_important_info['domain'] = ','.join(tweet['enriched']['url_details']['primary_domain'])
    name = None
    if tweet['retweet_status'] == 1:
        name = re.search('(?<=RT\s@)\w+(?=:)', tweet['text'].encode('utf-8')).group(0)
    tweet_important_info['twitter_RT'] = name
    try:
        tweet_important_info['user_listed_count'] = tweet['user']['listed_count']    
        tweet_important_info['tweet_geo'] = tweet['geo']
        tweet_important_info['tweet_coordinates'] = tweet['coordinates']
        tweet_important_info['tweet_place'] = tweet['place']
        tweet_important_info['tweet_favorite_count'] = tweet['favorite_count']
        tweet_important_info['tweet_retweet_count'] = tweet['retweet_count']
        tweet_important_info['tweet_favorited'] = tweet['favorited']
        tweet_important_info['tweet_retweeted'] = tweet['retweeted']
    except:
        pass
    return tweet_important_info



## Function: send messages to Kafka
    # create topics for notmached and error to review and debug codes if neccessary.
def send_to_kafka_matched(partition):
    kafka_prod = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
    for record in partition:
        kafka_prod.send('twitter_matched', str(json.dumps(record)))

def send_to_kafka_cleaned_matched(partition):
    kafka_prod = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
    for record in partition:
        kafka_prod.send('twitter_cleaned', str(record))

def send_to_kafka_notmatched(partition):
    kafka_prod = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
    for record in partition:
        kafka_prod.send('twitter_notmatched', str(record))

def send_to_kafka_err(partition):
    kafka_prod = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667')
    for record in partition:
        kafka_prod.send('twitter_err', str(record))

### Set up Cosmos DB connection
    # Write configuration
writeConfig = {
 "Endpoint" : "",
 "Masterkey" : "",
 "Database" : "",
 "Collection" : "",
 "Upsert" : "true"
}

    #Function: send data to Cosmos DB
import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents

def send_to_cosmos_db(partition):
    connectionPolicy = documents.ConnectionPolicy()
    connectionPolicy.EnableEndpointDiscovery 
    client = document_client.DocumentClient(writeConfig['Endpoint'], {'masterKey': writeConfig['Masterkey']}, connectionPolicy)

    dbLink = 'dbs/' + writeConfig['Database']
    collLink = dbLink + '/colls/' + writeConfig['Collection']
    for record in partition:
        client.CreateDocument(collLink, record)


#Function: Streaming context definition
def createContext():
    sc = SparkContext(appName=app_name)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, batchIntervalSec)
    

    # Define Kafka Consumer and Producer
    kafkaStream = KafkaUtils.createStream(ssc, 'sandbox-hdp.hortonworks.com:2181', app_name, {'twitter_streams':1})
    
    ## Get the JSON tweets payload
    ## if the Kafka message retrieved is not valid JSON the whole thing falls over
    tweets_dstream = kafkaStream.map(lambda v: json.loads(v[1]))

    ## -- Inbound Tweet counts
    inbound_batch_cnt = tweets_dstream.count()
    inbound_window_cnt = tweets_dstream.countByWindow(windowIntervalSec,batchIntervalSec)

    ## -- Process
    ## Match tweet to trigger criteria
    processed_tweets = tweets_dstream.map(lambda tweet:process_tweet(tweet))

    ## Send the matched data to Kafka topic

    matched_tweets = processed_tweets.filter(lambda processed_tweet:processed_tweet[0]>=1).map(lambda processed_tweet:processed_tweet[1])
    matched_tweets.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_matched))
    matched_batch_cnt = matched_tweets.count()
    matched_window_cnt = matched_tweets.countByWindow(windowIntervalSec,batchIntervalSec)
    
    ## Process tweet further (flatten json file and only select important features)
    clean_matched_tweets = matched_tweets.map(lambda tweet: process_matchedtweet(tweet))
    clean_matched_tweets.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_cosmos_db))
    
    clean_matched_tweets.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_cleaned_matched))
    clean_matched_batch_cnt = clean_matched_tweets.count()
    clean_matched_window_cnt = clean_matched_tweets.countByWindow(windowIntervalSec,batchIntervalSec)

    ## Send non-matched tweets to kafka topics to debug

    nonmatched_tweets = processed_tweets.filter(lambda processed_tweet:(-99<=processed_tweet[0]<1))
        
    nonmatched_tweets.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_notmatched))

    nonmatched_batch_cnt = nonmatched_tweets.count()
    nonmatched_window_cnt = nonmatched_tweets.countByWindow(windowIntervalSec,batchIntervalSec)

    ##  Print any erroring tweets
    ## Codes less than -100 indicate an error (try...except caught)

    errored_tweets = processed_tweets.filter(lambda processed_tweet:(processed_tweet[0]<=-100))
    errored_tweets.foreachRDD(lambda rdd: rdd.foreachPartition(send_to_kafka_err))

    errored_batch_cnt = errored_tweets.count()
    errored_window_cnt = errored_tweets.countByWindow(windowIntervalSec,batchIntervalSec)

    ## Set up count for matched keywords
    matched_keywords = matched_tweets.flatMap(lambda tweet: (tweet['enriched']['matched_words']))
    matched_keywords_batch_cnt = matched_keywords.countByValue()\
                                    .transform((lambda foo:foo.sortBy(lambda x:-x[1])))\
                                    .map(lambda x:"Batch/keywords: %s\tCount: %s" % (x[0],x[1]))

    matched_keywords_window_cnt = matched_keywords.countByValueAndWindow(windowIntervalSec,batchIntervalSec)\
                                    .transform((lambda foo:foo.sortBy(lambda x:-x[1])))\
                                    .map(lambda x:"Window/keywords: %s\tCount: %s" % (x[0],x[1]))
    
    ## Set up counts for Domains
    matched_domains = matched_tweets.flatMap(lambda tweet:(tweet['enriched']['url_details']['primary_domain']))

    matched_domains_batch_cnt = matched_domains.countByValue()\
                                    .transform((lambda foo:foo.sortBy(lambda x:-x[1])))\
                                    .map(lambda x:"Batch/Domain: %s\tCount: %s" % (x[0],x[1]))
    matched_domains_window_cnt = matched_domains.countByValueAndWindow(windowIntervalSec,batchIntervalSec)\
                                    .transform((lambda foo:foo.sortBy(lambda x:-x[1])))\
                                    .map(lambda x:"Window/Domain: %s\tCount: %s" % (x[0],x[1]))

    

    ## Print counts
    inbound_batch_cnt.map(lambda x:('Batch/Inbound: %s' % x))\
                    .union(matched_batch_cnt.map(lambda x:('Batch/Matched: %s' % x))\
                    .union(clean_matched_batch_cnt.map(lambda x: ('Batch/Cleaned: %s' % x))\
                    .union(nonmatched_batch_cnt.map(lambda x:('Batch/Non-Matched: %s' % x))\
                    .union(errored_batch_cnt.map(lambda x:('Batch/Errored: %s' % x)))))).pprint()

    inbound_window_cnt.map(lambda x:('Window/Inbound: %s' % x))\
                    .union(matched_window_cnt.map(lambda x:('Window/Matched: %s' % x))\
                    .union(nonmatched_window_cnt.map(lambda x:('Window/Non-Matched: %s' % x))\
                    .union(errored_window_cnt.map(lambda x:('Window/Errored: %s' % x))))).pprint()
    
    matched_keywords_batch_cnt.pprint()
    matched_keywords_window_cnt.pprint()
    matched_domains_batch_cnt.pprint()
    matched_domains_window_cnt.pprint()

    return ssc

     

if __name__=="__main__":
	ssc = StreamingContext.getOrCreate('/tmp/%s' % app_name,lambda: createContext())
	ssc.start()
	ssc.awaitTermination()

