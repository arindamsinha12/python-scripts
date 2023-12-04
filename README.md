# python-scripts

I have started to build up this repository to put down some of my learnings over years as a data professional.

While there are currently very few scripts, I will keep adding to this so please come back to check as the repository grows.

## Contents

### How to optimize performance of saving a Pandas dataframe to a Redshift table using parallelism and compression
Saving a dataframe to a Redshift table using Pandas df.to_sql is extremely slow.
This script provides a much faster method by using the following optimizations:
1) Break the dataframe into chunks and save them as separate csv files in parallel
2) Load the files to S3 in parallel
3) Load the files on S3 to the Redshift table in parallel using COPY command with file prefix
4) The CSV files are gzipped to make above three steps even faster

With the [sample dataset](https://github.com/arindamsinha12/python-scripts/tree/main/data), the
df.to_sql takes over 21 minutes to load the data to a Redshift table. With the above optimizations,
that loading time reduces to 45 seconds! For larger datasets, the gains will be much higher.
