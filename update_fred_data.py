from typing import List
import pandas as pd
import pandas_datareader as pdr
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
# import json
# import MySQLdb
from data_process.store_strategy import ProcessData
from dotenv import load_dotenv
import numpy as np
load_dotenv()


API_KEY_FRED = os.getenv('API_KEY_FRED')

# Function to write dataframe to SQL
def insert_dataframe_to_sql(table_name, dataframe, cursor):
    # Prepare the SQL insert statement
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    columns = ', '.join(dataframe.columns)

    # Prepare the ON DUPLICATE KEY UPDATE part of the query
    update_columns = ', '.join([f"{col} = VALUES({col})" for col in dataframe.columns])

    sql = f"""INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
              ON DUPLICATE KEY UPDATE {update_columns}"""

    # Convert dataframe to a list of tuples, handling NaN values
    data = [tuple(row) if not any(pd.isna(val) for val in row) 
            else tuple(None if pd.isna(val) else val for val in row) 
            for row in dataframe.values]

    # Execute the SQL command for each row
    cursor.executemany(sql, data)

def parse_release_dates(release_id: str) -> List[str]:
    release_dates_url = f'https://api.stlouisfed.org/fred/release/dates?release_id={release_id}&realtime_start=2015-01-01&include_release_dates_with_no_data=true&api_key={API_KEY_FRED}'
    r = requests.get(release_dates_url)
    text = r.text
    soup = BeautifulSoup(text, 'xml')
    dates = []
    for release_date_tag in soup.find_all('release_date', {'release_id': release_id}):
        dates.append(release_date_tag.text)
    return dates

econ_dfs = {}

econ_tickers = [
    'WALCL',
    'NFCI',
    'WRESBAL'
]

for et in tqdm(econ_tickers, desc='getting econ tickers'):
    df = pdr.get_data_fred(et)
    df.index = df.index.rename('ds')
    econ_dfs[et] = df

release_ids = [
    "10", # "Consumer Price Index"
    "46", # "Producer Price Index"
    "50", # "Employment Situation"
    "53", # "Gross Domestic Product"
    "103", # "Discount Rate Meeting Minutes"
    "180", # "Unemployment Insurance Weekly Claims Report"
    "194", # "ADP National Employment Report"
    "323" # "Trimmed Mean PCE Inflation Rate"
]

release_names = [
    "CPI",
    "PPI",
    "NFP",
    "GDP",
    "FOMC",
    "UNEMP",
    "ADP",
    "PCE"
]

releases = {}

for rid, n in tqdm(zip(release_ids, release_names), total = len(release_ids), desc='Getting release dates'):
    releases[rid] = {}
    releases[rid]['dates'] = parse_release_dates(rid)
    releases[rid]['name'] = n 

# Create a DF that has all dates with the name of the col as 1
# Once merged on the main dataframe, days with econ events will be 1 or None. Fill NA with 0
# This column serves as the true/false indicator of whether there was economic data released that day.
for rid in tqdm(release_ids, desc='Making indicators'):
    releases[rid]['df'] = pd.DataFrame(
        index=releases[rid]['dates'],
        data={
        releases[rid]['name']: 1
        })
    releases[rid]['df'].index = pd.DatetimeIndex(releases[rid]['df'].index)

df_releases = pd.concat([releases[k]['df'] for k in releases.keys()], axis=1)
df_releases = df_releases.fillna(0)
df_releases.index.name = 'Datetime'
df_releases = df_releases.astype(int)
df_releases.reset_index(inplace=True)

print(df_releases.head())
# Connect to the database

table_name = 'releases'  # Get the table name from the file name
chunks = np.array_split(df_releases, np.ceil(len(df_releases) / 500))
for data in tqdm(chunks, desc=f"storing data in {table_name}..."):
    p = ProcessData()
    insert_dataframe_to_sql = p.store_df(data, table_name)
# print(f"Data from {file_name} written to the table {table_name}")