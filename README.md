# scripts

I have started to build up this repository to put down some of my learnings over years as a data professional.

While there are currently very few scripts, I will keep adding to this so please come back to check as the repository grows.

## Contents

### 1. How to optimize performance of saving a Pandas dataframe to a Redshift table using parallelism and compression
Saving a dataframe to a Redshift table using Pandas df.to_sql is extremely slow.
This script provides a much faster method by using the following optimizations:
1. Break the dataframe into chunks and save them as separate csv files in parallel
2. Load the files to S3 in parallel
3. Load the files on S3 to the Redshift table in parallel using COPY command with file prefix
4. The CSV files are gzipped to make above three steps even faster

With the sample dataset [trade_transactions.csv](https://github.com/arindamsinha12/scripts/tree/main/data), the
df.to_sql ***takes over 21 minutes*** to load the data to a Redshift table. With the above optimizations,
that loading time ***reduces to 45 seconds!*** For larger datasets, the gains will be much higher.

### 2. Detect if a table exists without using information_schema
How do we check the existence of a table in a database? One option is to do a select from the
table within a try except block and analysing any exceptions.

However, a more elegant solution
is to try to create the table 'if not exists' with a dummy field name, and checking if the table
got created. Does require CREATE permission in the schema. This example is in Snowflake but the
method will work in any database.

### 3. Two ways of recursively finding a hierarchy in SQL
This SQL script shows two ways in which we can recursively find a hierarchy using a parent child
relationship between table columns. The two methods use (a) the START WITH...CONNECT BY construct
and (b) recursive CTE. Both methods should work in most modern databases though the
START WITH...CONNECT BY is not supported in all databases.

Table used for this example is this [employee_dataset.csv](https://github.com/arindamsinha12/scripts/blob/main/data/employee_dataset.csv)
