import argparse
import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import date, timedelta, datetime
import logging
import sys
import csv
import shutil
import os

def produce_output_csv(tweets_list, output_file_name):
      dest = './' + output_file_name

      tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])

      tweets_df.to_csv(dest, quoting=csv.QUOTE_ALL, mode='a', header=not os.path.exists(dest), index=False)

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_TWEETS = 2_000_000

parser = argparse.ArgumentParser(description='Scrap tweets')
parser.add_argument('date', type=str, help='Date to scrap tweets for')
args = parser.parse_args()

START_DATE = datetime.strptime(args.date, '%d-%m-%Y').date()
END_DATE = START_DATE + timedelta(days=1)

output_file_name = f"tweet_dump_{str(START_DATE)}.csv"

search_term = f"lang:en since:{str(START_DATE)} until:{str(END_DATE)}"
logger.info(f"Searching using search term: {search_term}")

# Creating list to append tweet data
tweets_list = []

# Using TwitterSearchScraper to scrape data and append tweets to list
for i,tweet in enumerate(sntwitter.TwitterSearchScraper(search_term).get_items()):
        if i>MAX_TWEETS: #number of tweets you want to scrape
            break
        if (i % 100 == 0):
            logger.info(f"<{str(START_DATE)}> Fetched {i} tweets")
        tweets_list.append([tweet.date, tweet.id, tweet.content, tweet.user.username]) #declare the attributes to be returned
        if (i > 0 and i % 50000 == 0):
            logger.info(f"Fetched 50_000 tweets, writing to disk")
            produce_output_csv(tweets_list, output_file_name)
            tweets_list = [] # Empty out list
