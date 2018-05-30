import re 


def sanitize_string(text): 
    pattern = re.compile(r'([^\s\w]|_)+', re.UNICODE)
    return ' '.join(pattern.sub('', text).split())