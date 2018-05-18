from utils.database import Database

import tkFileDialog as fd
import time, sys, os


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




