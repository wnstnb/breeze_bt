import requests
from io import BytesIO
import zipfile
import pandas as pd
from datetime import datetime
import pytz
from tqdm import tqdm
import os
import MySQLdb
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# URL of the API endpoint that returns the zip file
api_url = os.getenv("FIRSTRATEDATA_5MIN_INCR_API")

# Send a GET request to the API
response = requests.get(api_url)

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

# Connect to the database
connection = MySQLdb.connect(
  host=os.getenv("DATABASE_HOST"),
  user=os.getenv("DATABASE_USERNAME"),
  passwd=os.getenv("DATABASE_PASSWORD"),
  db=os.getenv("DATABASE"),
  autocommit=True,
  ssl_mode="VERIFY_IDENTITY",
  ssl={ "ca": "ca-certificates.crt" }
)

cursor = connection.cursor()

# Check if the request was successful
if response.status_code == 200:
    # Read the content of the response into a BytesIO object
    zip_content = BytesIO(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_content, "r") as zip_ref:
        # Iterate through the files in the zip archive
        dataframes = {}  # To store dataframes
        
        for file_name in tqdm(zip_ref.namelist()):
            if file_name in ['SPX_month_5min.txt','NDX_month_5min.txt','RUT_month_5min.txt']:
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

        # Write each dataframe to its corresponding SQL table
        for file_name, df in dataframes.items():
            table_name = file_name.replace('.txt', '')  # Get the table name from the file name
            table_name = table_name.replace('_month_', '_full_')  # swap names
            insert_dataframe_to_sql(table_name, df, cursor)
            print(f"Data from {file_name} written to the table {table_name}")

else:
    print("Failed to fetch the zip file.")

# Close the cursor and connection
cursor.close()
connection.close()

