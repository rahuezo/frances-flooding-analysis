import string, csv, re


KEYWORDS = [kw for kw in csv.reader(open('../resources/keywords.csv', 'rb'), delimiter=',')]

def is_flood_related(text): 
    pattern = re.compile(r'([^\s\w]|_)+', re.UNICODE)
    return ' '.join(pattern.sub('', text).split())