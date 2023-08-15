
import re
import datetime
from cleantext import clean


'''
  Cleans text from a.o. emojis, symbols, pictographs.
'''
def remove_emojis(text):
    emoji_pattern = re.compile("["
                    u"\U0001F600-\U0001F64F"  # emoticons
                    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                    u"\U0001F680-\U0001F6FF"  # transport & map symbols
                    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                    u"\U00002702-\U000027B0"
                    u"\U000024C2-\U0001F251"
                    u"\U0001f926-\U0001f937"
                    u'\U00010000-\U0010ffff'
                    u"\u200d"
                    u"\u2640-\u2642"
                    u"\u2600-\u2B55"
                    u"\u23cf"
                    u"\u23e9"
                    u"\u231a"
                    u"\u3030"
                    u"\ufe0f"
        "]+", flags=re.UNICODE)
    return (emoji_pattern.sub(r'', text)) 


'''
  Cleans text from a.o. special characters, new lines breaks, urls, 
  digits, numbers, punctuations, normalize to lowercase text.
  Helper library: https://pypi.org/project/clean-text/
'''
def clean_text(text):
    text_cleaned = clean(text,
        fix_unicode=True,               # fix various unicode errors
        to_ascii=True,                  # transliterate to closest ASCII representation
        lower=True,                     # lowercase text
        no_line_breaks=True,            # fully strip line breaks as opposed to only normalizing them
        no_urls=True,                   # replace all URLs with a special token
        no_emails=True,                 # replace all email addresses with a special token
        no_phone_numbers=True,          # replace all phone numbers with a special token
        no_numbers=True,                # replace all numbers with a special token
        no_digits=True,                 # replace all digits with a special token
        no_currency_symbols=True,       # replace all currency symbols with a special token
        no_punct=True,                  # remove punctuations
        replace_with_punct="",          # instead of removing punctuations you may replace them
        replace_with_url="",
        replace_with_email="",
        replace_with_phone_number="",
        replace_with_number="",
        replace_with_digit="",
        replace_with_currency_symbol="",
        lang="en"
    )
    return text_cleaned


'''
  Converts datetime to unix time format.
'''
def convert_to_unix_time(date_string):
    # Parse the date string using the specified format
    date_format = "%Y-%m-%d %H:%M:%S%z"# "%Y-%m-%d %H:%M:%S %Z"
    datetime_obj = datetime.datetime.strptime(date_string, date_format)

    # Convert the datetime object to Unix timestamp
    unix_time = datetime_obj.timestamp()
    return int(unix_time)