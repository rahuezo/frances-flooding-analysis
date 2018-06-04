from utils.configuration import (FIELDS_TO_SELECT_FOR_JOIN, MATCH_CRITERIA_FOR_JOIN, COASTAL_COUNTIES_TWEETS_TABLE)
from utils.validation import KEYWORDS
from utils.database import Database


import tkFileDialog as fd
import time, sys, os
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')

if not os.path.exists(RESULTS_PATH): 
    os.makedirs(RESULTS_PATH)


if __name__== "__main__":
    try: 
        coastal_counties_db_file = fd.askopenfilename(title="Choose database with coastal counties")
        coastal_counties_tweets_db_files = fd.askopenfilenames(title="Choose coastal counties tweets databases")
        

        if not coastal_counties_tweets_db_files or not coastal_counties_db_file: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    coastal_counties_db = Database(coastal_counties_db_file)    
    unique_counties = [fips[0] for fips in coastal_counties_db.select("""SELECT DISTINCT fips FROM counties_ngl""")] # array of text


    for i, tweet_db_file in enumerate(coastal_counties_tweets_db_files): 
        print '{} out of {} databases'.format(i + 1, len(coastal_counties_tweets_db_files))

        current_tweet_db = Database(tweet_db_file)

        current_tweet_db.cursor.execute(
            """
            SELECT fips FROM tweets
            WHERE CONTAINS()
            """
        )
        
        
        #cmd1 = "SELECT tweet_text FROM tweets WHERE fips = ".format(' OR fips = '.join(unique_counties))
        #Database(tweet_db_file).select("""SELECT tweet_text FROM tweets WHERE fips IN ()"")


        # coastal_counties_db.connection.close()
    







