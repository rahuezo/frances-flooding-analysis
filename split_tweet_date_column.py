from utils.database import Database
from utils.sanitation import sanitize_string

import tkFileDialog as fd
import time, sys, os, csv
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')

ALTER_TABLE_COMMAND = """
    ALTER TABLE tweets ADD COLUMN year INT; 
    ALTER TABLE tweets ADD COLUMN month INT; 
    ALTER TABLE tweets ADD COLUMN day INT; 
    ALTER TABLE tweets ADD COLUMN hour INT;
"""

UPDATE_COLUMN_COMMAND = """
    UPDATE tweets SET year = SUBSTR(tweet_date, 0, 5); 
    UPDATE tweets SET month = SUBSTR(tweet_date, 6, 2);
    UPDATE tweets SET day = SUBSTR(tweet_date, 9, 2);
    UPDATE tweets SET hour = SUBSTR(tweet_date, 12, 2);
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

        current_tweet_db.cursor.execute('BEGIN')
        
        try: 
            current_tweet_db.cursor.executescript(ALTER_TABLE_COMMAND) # split date into year, month, day, hour
        except: 
            print "\tColumns year, month, day, hour already exist."

        current_tweet_db.cursor.executescript(UPDATE_COLUMN_COMMAND) # split date into year, month, day, hour
        current_tweet_db.connection.commit()
    

        current_tweet_db.connection.close()

        end = time.time() - start
        total_time += end

        print "\tElapsed: {}s\tAverage: {}s".format(round(total_time, 2), round(total_time / (i+1), 2))

    print "\nTotal time: {}s".format(round(total_time, 2))


# UPDATE 
# tweets SET about_flood = CASE WHEN tweet_text like "%been%" THEN 1 ELSE 0
# END



# ALTER TABLE tweets ADD COLUMN year INT; 
# ALTER TABLE tweets ADD COLUMN month INT; 
# ALTER TABLE tweets ADD COLUMN day INT; 
# ALTER TABLE tweets ADD COLUMN hour INT; 


# UPDATE tweets SET year = SUBSTR(tweet_date, 0, 5); 
# UPDATE tweets SET month = SUBSTR(tweet_date, 6, 2);
# UPDATE tweets SET day = SUBSTR(tweet_date, 9, 2);
# UPDATE tweets SET hour = SUBSTR(tweet_date, 12, 2);