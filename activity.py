import config
import os
import pandas as pd
import tweepy as tweepy


# Twitter Authentication 
def authenicate(self):
    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit = True)

    return api

# Function that returns a dictionary of the total tweets for each symbol
def mostTweeted(self, sym, api):

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
def mostDiscussed(self, totalS):
    s = dict(sorted(totalS.items(), key=lambda x: x[1], reverse=True))

    top3 = []

    # Turn the ordered symbols into a list to iterate through (due to python3)
    for key in list(s.keys())[0:3]:
       top3.append(key)

    return top3

def sendTweet(self, symList, api):    
    api.update_status(status = "Three most discussed SPACs recently: {}".format(symList))
    
    
if __name__ == "__main__":
    api = authenicate()

    stocks = pd.read_csv("Units.csv")

    symbols = stocks['Issuer Symbol'] 
    symbols = symbols.dropna().reset_index(drop = 'True') # Drops rows with na values and resets the index 

    spacs = mostTweeted(symbols, api)
    top3 = mostDiscussed(spacs)
    sendTweet(top3, api)
