
from database import Database
from chunking import chunkify

import pandas as pd



# 2015_08_15

DATE_FORMAT = '%Y-%m-%d'
START_DATE = '{}-01-01'
END_DATE = '{}-12-31'

YEARS = [2014, 2015, 2016]
DAYS_IN_WEEK = 7



def generate_week_chunks(year):  
    start = pd.to_datetime(START_DATE.format(year), format=DATE_FORMAT)
    end = pd.to_datetime(END_DATE.format(year), format=DATE_FORMAT)    
    return chunkify([str(i.date()) for i in pd.date_range(start=start, end=end)], DAYS_IN_WEEK, smooth=True)


def generate_year_chunks(years): 
    return [generate_week_chunks(year) for year in years]


def week_chunks_to_db(db_filename): 
    db = Database(db_filename)
    weeks_table = db.create_table('weeks', 'start_date TEXT, end_date TEXT, week INT')

    db.cursor.execute('BEGIN')

    for year_chunk in generate_year_chunks(YEARS): 
        week_ranges = [(wchunk[0], wchunk[-1], i + 1) for i, wchunk in enumerate(year_chunk)]
        db.insert('INSERT INTO {} VALUES(?, ?, ?)'.format(weeks_table), week_ranges, many=True)
    
    db.connection.commit()
    db.connection.close()



def get_week_of_year(date, dates_db_filename): 
    dates_db = Database(dates_db_filename)
    week_number = dates_db.select('SELECT week FROM weeks WHERE start_date <= "{d}" AND end_date >= "{d}"'.format(d=date)).fetchone()[0]
    dates_db.connection.close()
    return week_number

def get_week_of_year_og(di, error_log=None):
    year = int(di.split('_')[0])
    
    di = di.replace('_', '-')[:-3] # get date in YYYY-MM-DD format
    
    d1 = datetime(year, 1, 1) # beginning date
    d2 = datetime(year + 1, 1, 1) # end date a year later
    
    delta = timedelta(days=1) # time delta is one day
    
    all_days = [str(i).split(' ')[0] for i in perdelta(d1, d2, delta)] # get all the days in a one year period
    
    week_of_year = {} 
    
    counter = 1
    
    for i in range(0, len(all_days) + 7, 7):
        current_week = tuple(all_days[i:i + 7]) # get the current 7 day week
        
        week_of_year[current_week] = counter # assign a week number to current week
        
        counter += 1 # increment the week number at end of iteration
    
    for woy in week_of_year:
        # if the given date is in one of the week intervals return the week number
        
        if di in woy:    
            return week_of_year[woy]
