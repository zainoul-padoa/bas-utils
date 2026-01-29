# This program cleans data about visit motives
# (G 24) Gefährdung der Haut
# (G24) Feuchtarbeit &#62;4 Std. pro Tag
# G 24
# G24
# -> merged to G24

import pandas as pd
import re

def extract_codes(combination):
    liste_combination = combination.split('|')
    liste_extracted = []
    for c in liste_combination:
        r = re.search('G\s?\d{1,2}\.?\d{0,2}', c)
        if r:
            liste_extracted.append(r[0].replace(' ', ''))
        else:
            liste_extracted.append(c)

    return ' | '.join(liste_extracted)

df = pd.read_excel('visit_combination_overall.xlsx')

df = df.drop('EN (AI-generated)',axis=1)
df['combination'] = df['combination'].apply(extract_codes)

df2=df.groupby('combination',as_index=False).agg(
    {'How many combinations together':'first',
    'standard combination or ad-hoc?':'first',
    'count':'sum',
    'Price with doubles':'first',
    'Price without doubles': 'first',
    'Absolute difference':'first',
    '% difference': 'first',
    'Price overall with doubles':'first',
    'Price overall without doubles':'first',
    'Absolute difference Overall':'first',
    'Comment': 'first'
    })

df2.to_excel('visit_combination_overall_2.xlsx')