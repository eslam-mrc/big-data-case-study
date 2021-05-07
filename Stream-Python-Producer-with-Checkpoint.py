import tweepy as tw
from pathlib import Path
import os
from time import sleep
from kafka import KafkaProducer
from json_tricks import dump, dumps, load, loads, strip_comments
import jsonpickle


#Twitter API credentials you get when you create an app
consumerKey = 'mKsFq1 . . . . . . . . . . .oawLY'
consumerSecret = 'Gz3TOr . . . . . . . . . . . . . . 8RDv0HuNl'
accessToken = '13824 . . . . . . . . . . . . . . . . . . . gJoE8JYFPD'
accessTokenSecret = '5IXpncm . . . . . . . . . . . . . . . . . U7aGR2Z88e'

#Create the authentication object
authenticate = tw.OAuthHandler(consumerKey, consumerSecret)

#Set the access token and access token secret
authenticate.set_access_token(accessToken, accessTokenSecret)

#Create the API object while passing in the auth information
api = tw.API(authenticate, wait_on_rate_limit = True)

#Searching tweets using a hashtag
#Define the search term and the date_since date as variables
search_words = "#SuperLeague"
date_since = "2021-04-10"

#This is to filter the retweets to avoid skewing your analysis
new_search = search_words + " -filter:retweets"

#The following list will contain the tweet IDs that we already replied to
tweetsAlreadyRead = list()

#This creates a checkpoint file that contains previously read tweet IDs if it doesn't exist\
# and throws no errors if it already exists
myfile = Path('./checkpoint.txt')
myfile.touch(exist_ok=True)

#Getting tweets that we read previosuly from the checkpoint file to avoid replying to the same tweets again
with open(myfile) as filehandle:
    tweetsAlreadyRead = [current_place.rstrip() for current_place in filehandle.readlines()]

#tweets is an iterable object that contains stuff like the text of the tweet, who sent the tweet, date and more
tweets = tw.Cursor(api.search, q=new_search, lang="en", since=date_since).items()

tweetsList = list()
record = {}
print("printing tweets ...")

#Data should be serialized before sending to Kafka topic
#Convert data to json file and encode it to utf-8
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


#I'm simulating the stream using while true
#I looked for the tweepy StreamListener way, the status object it returns has fewer attributes
while True:
    try:
        for tweet in tweets:
            #Checking whether we replied to this tweet before or not
            if tweet.id_str not in tweetsAlreadyRead:
                record = {"userID":tweet.user.id_str, "username":tweet.user.screen_name, "location":tweet.user.location, "followers":tweet.user.followers_count, "tweetID":tweet.id_str, "tweetText":tweet.text, "tweetDate":tweet.created_at}
                tweetsList.append(record)
                tweetsAlreadyRead.append(tweet.id_str)
                row = jsonpickle.encode(record)
                sleep(2)
                producer.send('testConnect', value=row)
                print("===========Row sent===========")
                #Writing each tweet we read to the checkpoint file
                with open('./checkpoint.txt', 'w') as filehandle:
                    filehandle.writelines("%s\n" % line for line in tweetsAlreadyRead)
                sleep(5)
    #Since we're using a free Twitter API, we're limited to a number of tweets per hour
    #Here I'm catching tweepy errors like the limit error    
    except tw.TweepError:
        sleep(60 * 15)
        continue
    except StopIteration:
        break
        






