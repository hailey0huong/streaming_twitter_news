'''
This script is to connect to Twitter API and send messages to Kafka Producer
'''

from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import json
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Fill out your own keys here. 
# They can be acquired by registering a Twitter Developer account

ckey = ''
consumer_secret = ''
access_token = ''
access_secret = ''

keywords = ['ML','Machine Learning','MachineLearning','machine learning',
			'DL', 'Deep Learning','DeepLearning','deep learning',
			'Big data','bigdata', 'NLP', 'Natural Language Processing', 
			'computer vision', 'ComputerVision', 'Artificial Intelligence', 'AI ']

class StdOutListener(StreamListener):
    def on_data(self, data):
    	try:
    		msg = json.loads( data )
    		print( msg['text'].encode('utf-8') )
        	producer.send("twitter_streams", data.encode('utf-8'))       	
        except BaseException as e:
        	print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print status
        return True

if __name__ == '__main__':
	producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'])
	l = StdOutListener()
	auth = OAuthHandler(ckey, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	stream = Stream(auth, l)
	stream.filter(languages=["en"], track = keywords)


