from pyspark.sql.functions import col

# Load customer data from Azure SQL
customer_data = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://<azure_sql_server_name>.database.windows.net:1433;database=<database_name>") \
    .option("query", "SELECT * FROM customers") \
    .option("user", "<username>") \
    .option("password", "<password>") \
    .load()

# Identify duplicate customers based on Name and Address
duplicate_customers = customer_data.groupBy("Name", "Address").count().filter("count > 1")

# Display duplicate customers
duplicate_customers.show()