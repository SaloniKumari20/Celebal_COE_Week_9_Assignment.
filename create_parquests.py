import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta

# Define the start and end date
start_date = datetime(2018, 1, 1)
end_date = datetime(2020, 10, 23)

# Generate a date range
date_range = pd.date_range(start_date, end_date)

# Function to generate dummy data and save as parquet
def create_parquet_file(date):
    # Create dummy data
    data = {
        'column1': np.random.rand(10),
        'column2': np.random.randint(0, 100, 10),
        'date': [date] * 10
    }
    
    # Create a DataFrame
    df = pd.DataFrame(data)
    
    # Define the file name
    file_name = f'TSK_{date.strftime("%Y%m%d")}.parquet'
    
    # Convert the DataFrame to a parquet file
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)

# Create parquet files for each date
for single_date in date_range:
    create_parquet_file(single_date)
