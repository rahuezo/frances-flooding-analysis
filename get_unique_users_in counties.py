from utils.database import Database

import tkFileDialog as fd
import time, sys, os, csv
import multiprocessing as mp


def load_shoreline_counties():
    with open(fd.askopenfilename(title="Choose shoreline counties file"), "rb") as inf: 
        return [row[0].zfill(5) for row in csv.reader(inf, delimiter=',')][1:]

relevant_counties = load_shoreline_counties()

if __name__== "__main__":
    try: 
        tweet_db_files = fd.askopenfilenames(title="Choose ALL databases with tweets")

        if not tweet_db_files: 
            raise Exception('\nNo databases selected! Goodbye.\n')
    except Exception as e: 
        print e
        sys.exit()

    # with open("190409 - unique user count per database.csv", "wb") as outf: 
        # writer = csv.writer(outf, delimiter=",")

        # writer.writerow(["Database", "Count"])

    counties = ','.join(['"{}"'.format(county) for county in relevant_counties])

    query = """SELECT DISTINCT user_id FROM tweets WHERE fips IN ({})""".format(counties)

    unique_users = set()

    for i, tweet_db_file in enumerate(tweet_db_files): 
        print '{} out of {} databases'.format(i + 1, len(tweet_db_files))

        current_db = Database(tweet_db_file)

        results = current_db.select(query)

        unique_users.update([result[0] for result in results])

    # writer.writerows([(db, count) for db, count in rows.items()])


    print "There are {} unique users.".format(len(unique_users))
    print "Finished!"