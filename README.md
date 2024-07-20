# Celebel-COE-Week-9
Weekly assignments for Celebel COE Data Engineering Week-9

Step 1: Load a random customer dataset to Azure SQL

Create an Azure SQL database and a table to store the customer data. Use Azure Data Factory (ADF) to load the customer dataset from a file storage (e.g., Azure Blob Storage) or an on-premises database to Azure SQL. Alternatively, you can use Azure Synapse Analytics (formerly Azure SQL Data Warehouse) to load the data.

Step 2: Identify duplicate customers based on Name and Address using Azure Databricks

Create an Azure Databricks workspace and a cluster. Load the customer data from Azure SQL to Azure Databricks using the Azure Databricks SQL connector. Use the deduplicate function in Azure Databricks to identify duplicate customers based on Name and Address.