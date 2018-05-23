from utils.configuration import (FIELDS_TO_SELECT_FOR_JOIN, MATCH_CRITERIA_FOR_JOIN, COASTAL_COUNTIES_TWEETS_TABLE)
from utils.database import Database


import tkFileDialog as fd
import time, sys, os


RESULTS_PATH = os.path.join(os.getcwd(), 'results')


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

        current_coastal_counties_db = Database(os.path.join(RESULTS_PATH, tweet_db_file))        
        coastal_counties_tweets_table = current_coastal_counties_db.create_table(*COASTAL_COUNTIES_TWEETS_TABLE)

        joined_rows = coastal_counties_db.ijoin((tweet_db_file, 'other_db', 'tweets'), FIELDS_TO_SELECT_FOR_JOIN + ',counties.fips', MATCH_CRITERIA_FOR_JOIN)

        print "Got rows"

        current_coastal_counties_db.cursor.execute('BEGIN')
        
        print "Inserting rows"
        current_coastal_counties_db.insert("""INSERT INTO {} 
            VALUES(?,?,?,?,?)""".format(coastal_counties_tweets_table), joined_rows, many=True)

        print "Commiting rows"

        current_coastal_counties_db.connection.commit()
        current_coastal_counties_db.connection.close()

    coastal_counties_db.connection.close()
    







