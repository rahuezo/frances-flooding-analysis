from sanitation import sanitize_string

import csv, re


KEYWORDS = [sanitize_string(kw[0].lower()) for kw in csv.reader(open('../resources/keywords.csv', 'rb'), delimiter=',')]

# static Regex("(?i)kw1|kw2", RegexOptions.Compiled)



def is_flood_related(text):    
    text = sanitize_string(text)
    for kw in KEYWORDS: 
        if kw in text: 
            return True
    return False