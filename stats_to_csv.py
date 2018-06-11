from utils.database import Database
from utils.sanitation import sanitize_string

import tkFileDialog as fd
import time, sys, os, csv
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')

UPDATE_COLUMN_COMMAND = """SELECT * FROM statistics"""

if not os.path.exists(RESULTS_PATH): 
    os.makedirs(RESULTS_PATH)


if __name__== "__main__":
    try: 
        coastal_counties_tweets_db_files = fd.askopenfilenames(title="Choose coastal counties tweets databases")
        
        if not coastal_counties_tweets_db_files: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    total_time = 0 
    total_time_loop = 0

    with open(os.path.join(RESULTS_PATH, "Aggregated Flood Tweet Statistics.csv"), 'wb') as output_file: 
        writer = csv.writer(output_file, delimiter=',')

        writer.writerow(['fips', 'year', 'month', 'day', 'hour', 'total tweets', 'total flood tweets', 'total users'])
        
        for i, tweet_db_file in enumerate(coastal_counties_tweets_db_files): 
            print '{} out of {} databases'.format(i + 1, len(coastal_counties_tweets_db_files))

            start = time.time()

            current_tweet_db = Database(tweet_db_file)    
            current_tweet_db.cursor.execute(UPDATE_COLUMN_COMMAND) # add new about_flood column default 0
                    
            writer.writerows(current_tweet_db.cursor)

            current_tweet_db.connection.close()

            end = time.time() - start
            total_time += end

            print "\tElapsed: {}s\tAverage: {}s".format(round(total_time, 2), round(total_time / (i+1), 2))

    print "\nTotal time: {}s".format(round(total_time, 2))
