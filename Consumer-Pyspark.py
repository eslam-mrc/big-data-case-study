from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from json_tricks import dump, dumps, load, loads, strip_comments
import jsonpickle
import pandas as pd
from textblob import TextBlob
import tweepy as tw
import time

#Twitter API credentials you get when you create an app, enter yours here
consumerKey = 'mKs . . . . . . . . . . awLY'
consumerSecret = 'Gz3 . . . . . . . . . . . . . . . KY98RDv0HuNl'
accessToken = '1382 . . . . . . . . . . . . . . . . . . . . . . . oE8JYFPD'
accessTokenSecret = '5IXp . . . . . . . . . . . . . . . . . . . .aGR2Z88e'

#Create the authentication object
authenticate = tw.OAuthHandler(consumerKey, consumerSecret)

#Set the access token and access
authenticate.set_access_token(accessToken, accessTokenSecret)

#Create the API object while passing in the auth information
api = tw.API(authenticate, wait_on_rate_limit = True)

#Creating a spark context
sc = SparkContext()


#To avoid unncessary logs
sc.setLogLevel("WARN")

#Batch duration, here I'm choosing to process for each second
ssc = StreamingContext(sc, 1)

#I created a sql context to be able to save parquet files to HDFS
sqlContext = SQLContext(sc)
 
kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['testConnect'], kafkaParams={"metadata.broker.list": "sandbox-hdp.hortonworks.com:6667"})

#This function takes a list of dictionaries and saves them as parquet files to HDFS
def saveParquet(listDict):
    df = sqlContext.createDataFrame(listDict)
    df.write.mode('append').parquet("hdfs://sandbox-hdp.hortonworks.com:8020/user/tweets-monitor/test-run")
    print("parquet done!")


#This function takes a dictionary object, do some simple analysis then reply to the tweet
def replyTweet(infoObject):
    tweet = infoObject["tweetText"]
    tweetID = infoObject["tweetID"]
    username = infoObject["username"]
    userID = infoObject["userID"]
    location = infoObject["location"]
    tweetDate = infoObject["tweetDate"]
    followers = infoObject["followers"]
    analysis = TextBlob(tweet)
    if (analysis.polarity > 0):
        reply = "Football is for the people "+username
        sentiment = "positive"
    elif (analysis.polarity < 0):
        reply = "You got that right! "+username
        sentiment = "negative"
    else:
        reply = "You've picked the wrong time to stay on the fence "+username
        sentiment = "neutral"

    try:
        api.update_status(status = reply , in_reply_to_status_id = tweetID , auto_populate_reply_metadata=True)
        print("Replied to user: ", username, "with ", reply)
        replyStatus = "Yes"
    except tw.TweepError as error:
        print('There was an error while replying to ', username, ' with error code: ', error.api_code)
        replyStatus = "No"
        pass

    rowDict = {"UserID":userID, "Username":username, "Location":location, "Followers":followers, "TweetID":tweetID, "TweetText":tweet, "CreatedAT":tweetDate, "Sentiment":sentiment, "Replied":replyStatus}
    return rowDict
    #print(rowDict)
    #saveParquet(rowDict) I'm leaving this here to remind me of the error I did not understand
    
    """
    #This is just for checking the shape of the data
    df = pd.DataFrame([rowDict])
    with open('/root/big-data-case-study/output.csv', 'a') as f:
        df.to_csv(f, header=f.tell()==0)
    print("Supposedly, I created the df")
    """
    
    
    
#This function takes a row (kafka message), extracts the value then decode its content and return a dictionary
def extract(row):
    info = row[1]
    infoString = loads(info).encode("utf-8")
    infoObject = jsonpickle.decode(infoString)
    #print(type(infoObject))
    return replyTweet(infoObject)

#This function takes an rdd, applies the extract function on each component of the rdd if the rdd is not empty\
# then because all of the previous steps were just transformations (just steps on a dag), I used a .collect()\
# to actually perform these steps and return a list of dictionaries that will be passed to the saveParquet function.
def handler(rdd):
    if not rdd.isEmpty():
        try:
            listDict = rdd.map(lambda row: extract(row)).collect()
            saveParquet(listDict)
        except:
            print("There was an error deserializing a tweet")
            pass


kafkaStream.foreachRDD(handler)

#Just a debugging print
#kafkaStream.pprint()


ssc.start()
ssc.awaitTermination()
