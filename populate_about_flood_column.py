from utils.database import Database
from utils.sanitation import sanitize_string

import tkFileDialog as fd
import time, sys, os, csv
import multiprocessing as mp


RESULTS_PATH = os.path.join(os.getcwd(), 'results')
KEYWORDS = [sanitize_string(kw[0].lower()).strip() for kw in csv.reader(open(fd.askopenfilename(title="Choose keywords file"), 'rb'), delimiter=',') if len(kw[0].lower()) > 0]

KEYWORDS_PATTERN = "tweet_text LIKE '% {kw} %' OR tweet_text LIKE '{kw} %' OR tweet_text LIKE '% {kw}_'"

UPDATE_COLUMN_COMMAND = """UPDATE tweets SET about_flood=1 WHERE {}""".format(" OR ".join([KEYWORDS_PATTERN.format(kw=kw) for kw in KEYWORDS]))

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

    for i, tweet_db_file in enumerate(coastal_counties_tweets_db_files): 
        print '{} out of {} databases'.format(i + 1, len(coastal_counties_tweets_db_files))

        start = time.time()

        current_tweet_db = Database(tweet_db_file)

        current_tweet_db.cursor.execute('BEGIN')
        current_tweet_db.cursor.execute(UPDATE_COLUMN_COMMAND) # add new about_flood column default 0
        current_tweet_db.connection.commit()
    

        current_tweet_db.connection.close()

        end = time.time() - start
        total_time += end

        print "\tElapsed: {}s\tAverage: {}s".format(round(total_time, 2), round(total_time / (i+1), 2))

    print "\nTotal time: {}s".format(round(total_time, 2))

# UPDATE 
# tweets SET about_flood = CASE WHEN tweet_text like "%been%" THEN 1 ELSE 0
# END