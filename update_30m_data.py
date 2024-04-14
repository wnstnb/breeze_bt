import requests
from io import BytesIO
import zipfile
import pandas as pd
from datetime import datetime
import pytz
from tqdm import tqdm
import os
# import MySQLdb
from data_process.store_strategy import ProcessData
from dotenv import load_dotenv
import numpy as np

# Load environment variables from the .env file
load_dotenv()

# URL of the API endpoint that returns the zip file
api_url = os.getenv("FIRSTRATEDATA_API")

# Send a GET request to the API
response = requests.get(api_url)

# Check if the request was successful
if response.status_code == 200:
    # Read the content of the response into a BytesIO object
    zip_content = BytesIO(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_content, "r") as zip_ref:
        # Iterate through the files in the zip archive
        dataframes = {}  # To store dataframes
        
        for file_name in tqdm(zip_ref.namelist()):
            if file_name in ['SPX_full_30min.txt','VIX_full_30min.txt','VVIX_full_30min.txt','NDX_full_30min.txt','RUT_full_30min.txt']:
                try:
                    # Read the text file from the zip archive
                    with zip_ref.open(file_name) as file:
                        lines = file.read().decode().splitlines()
                        
                        # Initialize lists to store data
                        timestamps = []
                        opens = []
                        highs = []
                        lows = []
                        closes = []
                        
                        for line in lines:
                            # Parse each line and extract the data
                            parts = line.split(",")
                            timestamp_str, open_price, high_price, low_price, close_price = parts
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                            # Convert to US Eastern Time timezone
                            # eastern_timezone = pytz.timezone("US/Eastern")
                            # timestamp_eastern = eastern_timezone.localize(timestamp)
                            
                            # timestamps.append(timestamp_eastern)
                            timestamps.append(timestamp)
                            opens.append(float(open_price))
                            highs.append(float(high_price))
                            lows.append(float(low_price))
                            closes.append(float(close_price))
                        
                        # Create a dataframe from the extracted data
                        df = pd.DataFrame({
                            "Datetime": timestamps,
                            "Open": opens,
                            "High": highs,
                            "Low": lows,
                            "Close": closes
                        })
                        
                        dataframes[file_name] = df

                except Exception as e:
                    print(f"An error occurred while processing {file_name}: {e}")
                    continue

        p = ProcessData()

        # Write each dataframe to its corresponding SQL table
        for file_name, df in dataframes.items():
            print(file_name, len(df), df.head())
            table_name = file_name.replace('.txt', '')  # Get the table name from the file name
            chunks = np.array_split(df, np.ceil(len(df) / 1000))
            for data in tqdm(chunks, desc=f"storing data in {table_name}..."):
                insert_dataframe_to_sql = p.store_df(data, table_name)
            # print(f"Data from {file_name} written to the table {table_name}")
else:
    print("Failed to fetch the zip file.")

