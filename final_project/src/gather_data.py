import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup


# Gather the contents of the webpage in the text format
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
table_id = 'By market capitalization'
html_data = requests.get(url).text

# Using BeautifulSoup to parse the contents of the webpage
soup=BeautifulSoup(html_data, 'html.parser')
table = soup.find_all('table')[3]

# Load the data from the "By market capitalization" table to a pandas DF

data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])

for row in table.find_all('tr'):
        col = row.find_all('td')
        if len(col) >= 3:
            if len(col[1].find_all('a')) == 2:
                name = col[1].find_all('a')[1].string
            elif len(col[1].find_all('a')) == 3:
                name = col[1].find_all('a')[2].string
            m_cap = col[2].string.strip()
            d = {"Name":[name], "Market Cap (US$ Billion)":[m_cap]}
            temp_array = pd.DataFrame(data=d)
            data = data.append(temp_array, ignore_index=True)

print(data.head())

data.to_json('../data/market_cap_data.json')