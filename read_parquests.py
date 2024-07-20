from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

def read_parquets(spark, start_date, end_date, base_path):
    # Convert string dates to datetime
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Generate the required year-month folders
    required_folders = []
    current_date = start_date
    while current_date <= end_date:
        folder_name = current_date.strftime('%Y%m')
        required_folders.append(folder_name)
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)
    
    # Read the parquets within the required date range
    dataframes = []
    for folder in required_folders:
        folder_path = os.path.join(base_path, folder)
        if os.path.exists(folder_path):
            parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
            for file in parquet_files:
                # Extract the date from the file name
                file_date = datetime.strptime(file.split('_')[1].split('.')[0], '%Y%m%d')
                if start_date <= file_date <= end_date:
                    df = spark.read.parquet(file)
                    dataframes.append(df)
    
    # Combine all dataframes
    if dataframes:
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)
        
        return combined_df
    else:
        return None

# Example usage in Databricks
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Read Parquets").getOrCreate()
    start_date = '2019-01-01'
    end_date = '2019-02-28'
    base_path = '/mnt/data'
    combined_df = read_parquets(spark, start_date, end_date, base_path)
    combined_df.show()
