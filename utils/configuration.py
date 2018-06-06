import os

from subprocess import call

print "Pulling updates...\n"
call(['git', 'pull'])



ROOT_DIR = os.getcwd().replace('utils', '')

TWEETS_TABLENAME = 'tweets'
FIELDS_TO_SELECT_FOR_JOIN = 'user_id, tweet_text, tweet_location, tweet_date'
MATCH_CRITERIA_FOR_JOIN = 'fips'
COASTAL_COUNTIES_TWEETS_TABLE = ('tweets', 'user_id INT, tweet_text TEXT, tweet_location TEXT, tweet_date TEXT, fips TEXT')

RESULTS_PATH = os.path.join(ROOT_DIR, 'results')

STATS_TB_COLUMNS = 'fips TEXT, year INT, month INT, day INT, hour INT, total_tweets INT, total_flood_tweets INT, users INT'



# if not os.path.exists(RESULTS_PATH): 
#     os.makedirs(RESULTS_PATH)

