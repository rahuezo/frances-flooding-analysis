from utils.configuration import (FIELDS_TO_SELECT_FOR_JOIN, MATCH_CRITERIA_FOR_JOIN, COASTAL_COUNTIES_TWEETS_TABLE)
from utils.database import Database


import tkFileDialog as fd
import time, sys, os
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')

if not os.path.exists(RESULTS_PATH): 
    os.makedirs(RESULTS_PATH)

CURRENT_COUNTIES_TABLE = 'counties'


if __name__== "__main__":
    try: 
        coastal_counties_db_file = fd.askopenfilename(title="Choose coastal counties database")
        tweet_db_files = fd.askopenfilenames(title="Choose ALL databases with tweets")
        

        if not coastal_counties_db_file or not tweet_db_files: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    coastal_counties_db = Database(coastal_counties_db_file)

    for i, tweet_db_file in enumerate(tweet_db_files): 
        print '{} out of {} databases'.format(i + 1, len(tweet_db_files))

        current_coastal_counties_db_file = os.path.join(RESULTS_PATH, os.path.split(tweet_db_file)[1])
        current_coastal_counties_db = Database(current_coastal_counties_db_file) 

        coastal_counties_tweets_table = current_coastal_counties_db.create_table(*COASTAL_COUNTIES_TWEETS_TABLE)

        joined_rows, other_db_name = coastal_counties_db.ijoin((tweet_db_file, 'other_db', 'tweets'), FIELDS_TO_SELECT_FOR_JOIN + ',{}.fips'.format(CURRENT_COUNTIES_TABLE), MATCH_CRITERIA_FOR_JOIN)
        current_coastal_counties_db.cursor.execute('BEGIN')
        
        current_coastal_counties_db.insert("""INSERT INTO {} 
            VALUES(?,?,?,?,?)""".format(coastal_counties_tweets_table), joined_rows, many=True)

        current_coastal_counties_db.connection.commit()
        coastal_counties_db.cursor.execute("""DETACH DATABASE '{}'""".format(other_db_name))
        current_coastal_counties_db.connection.close()

    coastal_counties_db.connection.close()
    







