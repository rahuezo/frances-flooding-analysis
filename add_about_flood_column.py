from utils.database import Database


import tkFileDialog as fd
import time, sys, os
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')
ADD_COLUMN_COMMAND = """ALTER TABLE tweets ADD COLUMN about_flood INT DEFAULT 0"""
RESET_ABOUT_FLOOD_COLUMN_COMMAND = """UPDATE tweets SET about_flood=0"""

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

        try: 
            current_tweet_db.cursor.execute('BEGIN')
            current_tweet_db.cursor.execute(ADD_COLUMN_COMMAND) # add new about_flood column default 0
            current_tweet_db.connection.commit()
        except Exception as e: 
            print "\t{}".format(e)
            print "\tColumn has already been added! Resetting about_flood to 0.\n"
            current_tweet_db.cursor.execute('BEGIN')
            current_tweet_db.cursor.execute(RESET_ABOUT_FLOOD_COLUMN_COMMAND) # add new about_flood column default 0
            current_tweet_db.connection.commit()
            continue

        current_tweet_db.connection.close()

        end = time.time() - start
        total_time += end

        print "\tElapsed: {}s\tAverage: {}s".format(round(total_time, 2), round(total_time / (i+1), 2))

    print "\nTotal time: {}s".format(round(total_time, 2))





# ALTER TABLE tweets ADD COLUMN about_flood INT DEFAULT 0; 

# UPDATE 
# tweets SET about_flood = CASE WHEN tweet_text like "%been%" THEN 1 ELSE 0
# END