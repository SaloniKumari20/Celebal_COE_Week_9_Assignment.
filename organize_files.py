import os
import shutil
from datetime import datetime
import pandas as pd

# Define the start and end date
start_date = datetime(2018, 1, 1)
end_date = datetime(2020, 10, 23)

# Generate a date range
date_range = pd.date_range(start_date, end_date)

# Function to move parquet files to respective folders
def organize_files_by_month():
    for single_date in date_range:
        # Define the folder name based on Year-Month
        folder_name = single_date.strftime('%Y%m')
        
        # Create the folder if it doesn't exist
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        
        # Define the file name
        file_name = f'TSK_{single_date.strftime("%Y%m%d")}.parquet'
        
        # Move the file to the respective folder
        if os.path.exists(file_name):
            shutil.move(file_name, os.path.join(folder_name, file_name))

# Organize the files
organize_files_by_month()
