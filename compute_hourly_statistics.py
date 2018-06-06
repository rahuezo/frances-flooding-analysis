from utils.configuration import STATS_TB_COLUMNS
from utils.database import Database


import tkFileDialog as fd
import time, sys, os
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')

INSERT_GROUP_BY_COMMAND = """
    INSERT INTO {tbn}
    SELECT fips, year, month, day, hour, 
    count(*) total_tweets, count(CASE WHEN about_flood=1 THEN about_flood END) total_flood_tweets, 
    count(DISTINCT user_id) users 
    FROM tweets group by fips, year, month, day, hour
"""

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

    for i, tweet_db_file in enumerate(coastal_counties_tweets_db_files): 
        print '{} out of {} databases'.format(i + 1, len(coastal_counties_tweets_db_files))

        start = time.time()

        current_tweet_db = Database(tweet_db_file)

        stats_tb = current_tweet_db.create_table('statistics', STATS_TB_COLUMNS)

        current_tweet_db.cursor.execute('BEGIN')
        current_tweet_db.cursor.executescript(INSERT_GROUP_BY_COMMAND.format(tbn=stats_tb)) # create statistics table with groub by selection
        current_tweet_db.connection.commit()
    
        current_tweet_db.connection.close()

        end = time.time() - start
        total_time += end

        print "\tElapsed: {}s\tAverage: {}s".format(round(total_time, 2), round(total_time / (i+1), 2))

    print "\nTotal time: {}s".format(round(total_time, 2))


# SELECT fips, year, month, day, hour, 
# count(*) total_tweets, count(CASE WHEN about_flood=1 THEN about_flood END) total_flood_tweets, 
# count(DISTINCT user_id) users 
# FROM tweets group by fips, year, month, day, hour
    







