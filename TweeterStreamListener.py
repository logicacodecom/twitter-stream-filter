import json
import tweepy
import configparser
import logging

import boto3
from datetime import datetime
import calendar
import random
import time
import sys
import tweepy
#from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#from lib import twitter_stream.FilteredStream
#import lib
#from twitter_stream import FilteredStream

logger = logging.getLogger(__name__)



config = configparser.ConfigParser(interpolation=None)

config.read('settings.txt')

consumer_key = config['TWITTER']['consumer_key']
consumer_secret = config['TWITTER']['consumer_secret']
access_token = config['TWITTER']['access_token']
access_token_secret = config['TWITTER']['access_token_secret']
bearer_token = config['TWITTER']['bearer_token']

aws_region_name = config['AWS']['region_name']
aws_access_key_id =  config['AWS']['access_key_id']
aws_secret_access_key = config['AWS']['secret_access_key']
aws_kinesis_stream_name = config['AWS']['kinesis_stream_name']

search_input= "nba, #NBA, NBA, #nba, Nba"

# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.



class TweeterStreamListener(tweepy.StreamingClient):        
    # on success
    def on_data(self, data):
        tweet = json.loads(data)
        try:
            if 'text' in tweet.keys():
                #print (tweet['text'])
                # message = str(tweet)+',\n'
                message = json.dumps(tweet)
                message = message + ",\n"
                print(message)
                '''
                kinesis_client.put_record(
                    DeliveryStreamName=aws_kinesis_stream_name,
                    Record={
                    'Data': message
                    }
                )
                '''
        except (AttributeError, Exception) as e:
                print (e)
        return True
    
    def on_status(self, status):
        print(status.id)

    def on_tweet(self, tweet):
        print(tweet.id)

    # on failure
    def on_error(self, status):
        print(status)




if __name__ == '__main__':
    # create kinesis client connection
    '''
    kinesis_client = boto3.client('firehose',
                                  region_name=aws_region_name,  # enter the region
                                  aws_access_key_id=aws_access_key_id,  # fill your AWS access key id
                                  aws_secret_access_key=aws_secret_access_key)  # fill you aws secret access key
    '''
    # create instance of the tweepy tweet stream listener
    print("bearer_token = "+bearer_token)

    # using get_user with id
    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    id = "869660137"
    user = client.get_user(id=id)
    print(f"The user name for user id {id} is {user.data.name}.")

    # Search for queries in English language with 'elon musk' that are not retweets
    query = 'elon musk lang:en -is:retweet'

    # Granularity can be minute, hour, day
    counts = client.get_recent_tweets_count(query=query, granularity='day')

    for count in counts.data:
        print(count)
    # authentication
    #auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    #auth.set_access_token(access_token, access_token_secret)

    #api = tweepy.API(auth)
    #print(api.get_place_trends)
    listener = TweeterStreamListener(bearer_token=bearer_token)
    # set twitter keys/tokens
    #auth = OAuthHandler(consumer_key, consumer_secret)
    #auth.set_access_token(access_token, access_token_secret)
    # create instance of the tweepy stream
    #stream = Stream(auth, listener)
    # search twitter for tags or keywords from cli parameters
    query = search_input
    #query_fname = ' '.join(query) # string
    #listener.filter(track=query)
    rules = listener.get_rules()
    for rule in rules:
        listener.delete_rules(rules)

    
    listener.add_rules(tweepy.StreamRule("apple lang:en"))
    listener.filter()
    #listener.sample()
    #listener.filter() #runs the stream

