"""Address processing utilities."""

import re
import pandas as pd
from unidecode import unidecode


def clean_german_road(text):
    """Clean German road names for matching."""
    if pd.isna(text) or text == '':
        return None
    text = text.lower().strip()
    text = text.replace('ß', 'ss')
    text = re.sub(r'(str\.|str$|str\s|straße|strasse)', '', text)
    text = unidecode(text)
    text = re.sub(r'[^a-z0-9]', '', text)
    return text


def split_and_clean_house_number(val):
    """Split and clean house numbers, handling ranges like '62-64' or '62/64'."""
    if pd.isna(val) or val == '':
        return pd.Series([None, None])
    
    # Split the string if it contains '-' or '/'
    parts = re.split(r'[-/]', str(val))
    
    # Helper to remove all non-numeric characters
    def keep_only_digits(s):
        cleaned = re.sub(r'\D', '', s)
        return cleaned if cleaned != '' else None

    # Process the parts
    num_1 = keep_only_digits(parts[0])
    num_2 = None
    
    # If there was a separator, process the second part
    if len(parts) > 1:
        num_2 = keep_only_digits(parts[1])
        
    return pd.Series([num_1, num_2])
