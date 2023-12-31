# Useful Python and SQL code patterns

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
How do we check the existence of a table in a database without using system tables in information_schema?
One option is to do a select from the table within a try except block and analysing any exceptions.

However, a more elegant solution is to try to create the table 'if not exists' with a dummy field name, and
checking if the table got created. Does require CREATE permission in the schema. This example is in Snowflake
but the method will work in any database.

### 3. Two ways of recursively finding a hierarchy in SQL
This SQL script shows two ways in which we can recursively find a hierarchy using a parent child
relationship between table columns. The two methods use (a) the START WITH...CONNECT BY construct
and (b) recursive CTE. Both methods should work in most modern databases though the
START WITH...CONNECT BY is not supported in all databases.

Table used for this example is this [employee_dataset.csv](https://github.com/arindamsinha12/scripts/tree/main/data)

### 4. Python job scheduler

I had a situation where our legacy scheduling platform Talend would have license expiry coming up, while the new
scheduling in Airflow in the new platform wasn't quite ready to go. There was a period of about 3 months during
which the jobs on the legacy platform had to continue to run. To bridge the scheduling gap, I used Python scheduling
for the time being.

This demonstrates a number of Python features I used:
- Execution of jobs from a continuously running scheduler (should be run in background)
  - Config file driven - allows dynamically changing parameters that may need changing without stopping the scheduler
  - Threaded execution to ensure scheduler does not error out if any of the jobs fail
  - Passing parameters to threaded jobs and scheduled jobs
- Sending HTML alert emails. For this example I have used Gmail using an app password that has to be set up separately
  - See https://support.google.com/accounts/answer/185833?hl=en for setting up app password
  - In a corporate setting a password usually won't be required
- Usage of AWS Secrets Manager to retrieve app password instead of putting it in config file. In a corporate
  environment, this could be for database credentials etc.
- Practical use of Python eval to read in a function name from config file and execute that function

Python scheduling is not meant to replace an actual scheduler as it lacks functionality like
history of job runs etc., but for an intermediate period it is more than adequate, especially if properly parameterized.
This script demonstrates the principle by sending HTML emails rather than doing any actual database activity which
would typically be what would need to be done in Production.

Multiple jobs can be scheduled using this script, based on an YAML config file.

Parameters can be passed via the YAML config file. However, things like database credentials should not be put directly
in the file. It could contain a secret name (e.g. for AWS Secrets Manager or Databricks Secrets utility), which in turn
should be used by the job (which in this case is defined as a function) to obtain the credentials.

Sample config file [sched_config.yml](https://github.com/arindamsinha12/scripts/tree/main/config) is used for
this example.

