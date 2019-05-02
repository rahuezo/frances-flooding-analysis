from utils.database import Database

import tkFileDialog as fd
import time, sys, os
import multiprocessing as mp

relevant_counties = ["48245", "48361"]


if __name__== "__main__":
    try: 
        tweet_db_files = fd.askopenfilenames(title="Choose ALL databases with tweets")

        if not tweet_db_files: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()


    for i, tweet_db_file in enumerate(tweet_db_files): 
        print '{} out of {} databases'.format(i + 1, len(tweet_db_files))

        current_db = Database(tweet_db_file)

        query = """SELECT * FROM tweets WHERE fips IN ("{}", "{}") AND about_flood=1""".format(relevant_counties[0], relevant_counties[1])

        for row in current_db.select(query): 
            print row

        

        
    







