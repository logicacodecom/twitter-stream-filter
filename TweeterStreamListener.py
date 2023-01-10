import json
#from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import tweepy
import configparser
from pymongo import MongoClient
import logging


#from dotenv import load_dotenv

#load_dotenv()

logger = logging.getLogger(__name__)



config = configparser.ConfigParser()
config.read('settings.txt')

consumer_key = config['TWITTER']['consumerKey']
consumer_secret = config['TWITTER']['consumerSecret']
access_key = config['TWITTER']['accessToken']
access_secret = config['TWITTER']['accessTokenSecret']
bearerToken = config['TWITTER']['accessTokenSecret']

bootstrap_servers = config['KAFKA']['bootstrap_servers']
kafka_topic_name = config['KAFKA']['kafka_topic_name']
#kafka_topic_name="twitter_topic" #name of kafka's server topic name, noit topics to search in 

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

search_input= "nba, #NBA, NBA, #nba, Nba"


""" class TwitterStreamer():
    def stream_data(self):
        logger.info(f"{kafka_topic_name} Stream starting for {search_input}...")

        twitter_stream = TweeterStreamListener(consumer_key, consumer_secret, access_token, access_token_secret)
        twitter_stream.filter(track=[search_input]) """

# Note: Some of the imports are external python libraries. They are installed on the current machine.
# If you are running multinode cluster, you have to make sure that these libraries
# and currect version of Python is installed on all the worker nodes.


class TweeterStreamListener(tweepy.StreamingClient):
    """ A class to read the twitter stream and push it to Kafka"""

    """ def __init__(tweepy.Stream):
        config = configparser.ConfigParser()
        config.read('settings.txt')
        bootstrap_servers = config['KAFKA']['bootstrap_servers']

        kafka_topic_name = config['KAFKA']['kafka_topic_name']
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers) """

        #self.api = api
        #self.topic = topic

        #super(tweepy.Stream, self).__init__()
        #self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        #self.producer = KafkaProducer(
        #    value_serializer=lambda m: json.dumps(m).encode('ascii'))
        #self.producer = SimpleProducer(client, async = True,
        #                  batch_send_every_n = 1000,
        #                  batch_send_every_t = 10)
    def on_connect(self):
        print(">>>>> Twitter Connected")
        return super().on_connect()

    def on_tweet(self, tweet):
        print(">>>>> Twitter On Tweet = "+tweet)
        return super().on_tweet(tweet)
    
    

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        try:
            print(kafka_topic_name)
            producer.send(str(kafka_topic_name), status._json)
        except Exception as e:
            print(e)
            return False
        return True
    
    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        try:
            print(data)
            producer.send(kafka_topic_name, data.encode('utf-8')) #chasnge to str(self.topic) instead of kafka_topic_name
            return True
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer:" )
        print(status_code)
        return True  # Don't kill the stream

    def on_timeout(self):
        return True  # Don't kill the stream


def mongoreadid(collection, field):
    """ reads a specific field in a specific collection from mongodb """
    config = configparser.ConfigParser()
    config.read('settings.txt')
    mongo_url = config['MONGO']['url']
    client = MongoClient(mongo_url)
    db = client['admin']
    ids = db[collection]
    return list(map((lambda id: id[field]), ids.find()))

""" 
def stream_id(id):
    return tweepy.Stream(auth, listener=TweeterStreamListener(api, id)) """


""" def mongoread(database, collection, field):
    #reads a specific field in a specific collection from mongodb 
    config = configparser.ConfigParser()
    config.read('twitter-app-credentials.txt')
    mongo_url = config['MONGO']['url']
    client = MongoClient(mongo_url)
    db = client[database]
    ids = db[collection]
    return list(map((lambda id: id[field]), ids.find())) """


if __name__ == '__main__':

    # Read the credententials from 'twitter-app-credentials.txt' file
    # config = configparser.ConfigParser()
    # config.read('settings.txt')
    # consumer_key = config['TWITTER']['consumerKey']
    # consumer_secret = config['TWITTER']['consumerSecret']
    # access_key = config['TWITTER']['accessToken']
    # access_secret = config['TWITTER']['accessTokenSecret']

    # Create Auth object
    #auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    #auth.set_access_token(access_key, access_secret)
    #api = tweepy.API(auth)

    # Create stream and bind the listener to it
    #stream = tweepy.Stream(auth, listener = TweeterStreamListener(api,""))

    #stream_now = TweeterStreamListener(consumer_key, consumer_secret, access_key, access_secret)
    streaming_client = TweeterStreamListener(bearer_token=bearerToken)
    #streaming_client.add_rules(tweepy.StreamRule("Tweepy"))
    streaming_client.sample()


    #stream_now.add_rules(tweepy.StreamRule("#Bitcoin")) #adding the rules
    #stream_now.filter() #runs the stream
    #Custom Filter rules pull all traffic for those filters in real time.
    #stream_now.filter(track = ['love', 'hate'])
    #stream_now.filter(locations=[-180,-90,180,90], languages = ['en'])

    """  
    # Read the users' ids form mongodb
    id_list = mongoreadid('ids', 'id')
    # Read the topics form mongodb
    topic_list = mongoreadid('topics', 'topic')
    topic_list = ['cars']
    # Create stream and bind the listener to it
    stream = list(map(lambda id: stream_id(id), id_list))
    # Custom Filter rules pull all traffic for those filters in real time.
    # stream.filter(track = ['love', 'hate'], languages = ['en'])
    keywords = mongoread('admin', 'keywords', 'keywords')
    try:
        stream[0].filter(track=['madrid'], languages=['en'])
        stream[1].filter(track=['neymare'], languages=['en'])
        # map(lambda id : stream[id].filter(track=topic_list, languages = ['en']) , id_list)
    except Exception as e:
        print(e) """
