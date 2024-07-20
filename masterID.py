from pyspark.sql.functions import row_number

# Assign Master ID to matching customers
master_id_assigned = duplicate_customers.withColumn("MasterId", row_number().over(Window.partitionBy("Name", "Address").orderBy("CustomerId")))

# Display Master ID assigned customers
master_id_assigned.show()