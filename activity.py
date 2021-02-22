import config
import os
import pandas as pd
import tweepy as tweepy


# Twitter Authentication 
def authenicate():
    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit = True)

    return api

# Function that returns a dictionary of the total tweets for each symbol
def mostTweeted(sym, api):

    totalSymbols = {}

    # Calculates the total number of tweets for each symbol 
    # Note: Tweepy API does NOT support an exhuast on the queries, meaning I wont get ALL the tweets within the time frame if it's too large.

    for s in sym[0:4]:
        tweetCount = api.search("${}".format(s))
        count = 0

        for tweet in tweetCount:
            text = tweet.text
            if text[0:2] != 'RT':
                count += 1

        totalSymbols[s] = count
    
    print(totalSymbols) # Test to see if all SPACs tweets have been counted

    # Returns a dictionary of the symbol and total tweets for that symbol
    return totalSymbols
    
# returns a list of the 3 most discussed symbols
def mostDiscussed(totalS):
    s = dict(sorted(totalS.items(), key=lambda x: x[1], reverse=True))

    top3 = []

    # Turn the ordered symbols into a list to iterate through (due to python3)
    for key in list(s.keys())[0:3]:
       top3.append(key)

    return top3

def sendTweet(symList, api):    
    api.update_status(status = "Three most discussed SPACs recently: {}".format(symList))
    
    
if __name__ == "__main__":
    api = authenicate()

    stocks = pd.read_csv("Units.csv")

    symbols = stocks['Issuer Symbol'] 
    symbols = symbols.dropna().reset_index(drop = 'True') # Drops rows with na values and resets the index 

    spacs = mostTweeted(symbols, api)
    top3 = mostDiscussed(spacs)
    sendTweet(top3, api)
    
"""

- Scrape all tweets of the symbols (for loop of the range of the symbols)
- Put the amount od times the symbol appears in the dictonary into
- https://docs.tweepy.org/en/latest/api.html


- Then do the stuff below:

    (Need to make it so the script always runs as well)
        - a CRON script will allow us to run time at a set time
                - NOTE: This is a macbook SCHEDULER --> so clone this git repo (when uploaded and run this on my MAC) --> This should work for tweeting out scheduled outputs 

                - NOTE: CRON ONLY!!!! runs WHEN MAC IS ON --> ANACRON can schedule scripts when mac is shut down

    1. Figure out how to see if one of the SPAC symbols is trending (maybe find a way to fetch like every 5 mins)
            - Reads the trending 
    2. Then figure out how to write the trends to a twitter account to tweet


    Batch processing --> (Apache Spark) --> NOT processed in real time, meaning that it can take like a day to process the data
    Real Time Processing --> (Apache Kafka) --> Processes the data the day of and is used for real time analytics

    Airflow --> Purpose is to schedule jobs --> Directed Acyclic Graphs or DAGs are different jobs that are scheduled (might not be fully correct)
            - Can use Apache Spark with airflow to schedule jobs to perform in PARELLEL

"""