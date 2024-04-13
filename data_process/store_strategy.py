import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
import numpy as np
import json

load_dotenv()

class ProcessData:
    def __init__(self):
        url: str = os.getenv("SUPABASE_URL")
        key: str = os.getenv("SUPABASE_KEY")
        self.supabase: Client = create_client(url, key)

    def store_df(self, df_store: pd.DataFrame):
        try:
            # Convert Timestamp, Timedelta and int32 objects to strings or int
            df_store = df_store.apply(lambda x: x.apply(lambda y: y.isoformat() if isinstance(y, (pd.Timestamp, pd.Timedelta)) else int(y) if isinstance(y, np.int32) else y))

            # Convert the DataFrame to a list of dictionaries
            data = df_store.to_dict('records')

            # Try to convert each element to JSON
            for record in data:
                for key, value in record.items():
                    try:
                        json.dumps(value)
                    except TypeError:
                        return f"An error occurred: Object of type {type(value).__name__} in column '{key}' is not JSON serializable"

            # Write the data to the 'strategy_stats' table
            response = self.supabase.table('strategy_stats').upsert(data).execute()

            # Check if the request was successful
            if response.error is None:
                return 'Data written to the database successfully.'
            else:
                return f"An error occurred: {response.error.message}"
        except Exception as e:
            return f"An error occurred: {str(e)}"