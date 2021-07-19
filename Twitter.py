#!/usr/bin/python
import tweepy
import csv #Import csv
import json
import re
import csv
import time


def clean_tweet(tweet):
    tweet2 = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())
    tweet3 = re.sub(r'^RT[\s]+', '', tweet2)
    return tweet3




auth = tweepy.auth.OAuthHandler('YCRvBi8r0LNZdb4WuYL4bNIEJ', 'VzdKny2yud9vahUfBdnYcJZmwAzFu4NC2KJmqTZEM5CBjSHbPZ')
auth.set_access_token('1401678034625761280-oCm6rlANlvljtBU5Eso8rQeTGYY5Ae', 'wdkiNklaqpCn4gKTzNd7x1WqZfWb5DOrY2n1LpUVSVz3W')

api = tweepy.API(auth)

csvFile = open('resultadoFinal.csv', 'a')

csvWriter = csv.writer(csvFile)
search_words = ["i want to die","i dont want to live anymore","i will kill myself","live","fucking","anyone","bad","shit","tried","suicidal","pain","wish","enough","wanted","die","death","fuck","i dont care","i want to die"]
numberOfTweets = 300
for x in search_words:
    print(x)
    for tweet in tweepy.Cursor(api.search,
                               q =  x,
                              # since = "2014-02-14",
                               #until = "2014-02-15",
                               lang = "en").items(400):

        time.sleep(1)
        twitter_limpo = clean_tweet(tweet.text)
        csvWriter.writerow([tweet.user.screen_name,tweet.created_at, twitter_limpo])
        print(tweet.user.screen_name,tweet.created_at, twitter_limpo)
csvFile.close()