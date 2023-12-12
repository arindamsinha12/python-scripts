#!/usr/bin/env python

"""fast_save_df_to_redshift.py: Save a Pandas dataframe faster to Redshift Pandas to_sql does."""

"""
Saving a dataframe to a Redshift table using Pandas df.to_sql is extremely slow.
This script provides a much faster method by using the following optimizations:
1) Break the dataframe into chunks and save them as separate csv files in parallel
2) Load the files to S3 in parallel
3) Load the files on S3 to the Redshift table in parallel using COPY command with file prefix
4) The CSV files are gzipped to make above three steps even faster

With the sample dataset trade_transactions.csv in https://github.com/arindamsinha12/scripts/tree/main/data, the
df.to_sql takes over 21 minutes to load the data to Redshift. With the above optimizations,
that loading time reduces to 45 seconds! For larger datasets, the gains will be much higher.
"""

# Requires installation of sqlalchemy-redshift and redshift_connector
# pip install sqlalchemy-redshift
# pip install redshift_connector

import multiprocessing
import os
import shutil
import pandas as pd
import boto3
import json
import csv
import numpy as np
import redshift_connector
import sqlalchemy as sa

from sqlalchemy.engine.url import URL
from botocore.exceptions import ClientError
from datetime import datetime

__author__ = "Arindam Sinha"
__license__ = "GPL"
__version__ = "1.0.0"
__status__ = "Prototype"

# Get the IAM Role from AWS Secrets Manager to authorize the COPY command
def get_secret_iam_role():
    secret_name = "<<IAM Role Secret Name>>"
    region_name = "us-west-2"


    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])

    # Your code goes here.
    iam_role_copy = secret['iam_role_copy_command_access']

    return iam_role_copy


# Get the Redshift credentials from AWS Secrets Manager to connect to Redshift
def get_secret_creds():
    secret_name = "<<Redshift Credentials Secret Name>>"
    region_name = "us-west-2"


    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = json.loads(get_secret_value_response['SecretString'])

    # Get the credentials
    hst = secret['host']
    prt = secret['port']
    dbnm = secret['dbName']
    usr = secret['username']
    pwd = secret['password']

    return hst, prt, dbnm, usr, pwd


# Create a sqlalchemy engine
def get_engine(hst, prt, dbnm, usr, pwd):
    # build the sqlalchemy URL
    url = URL.create(
        drivername='redshift+redshift_connector',  # indicate redshift_connector driver and dialect will be used
        host=hst,
        port=prt,
        database=dbnm,
        username=usr,
        password=pwd
    )

    engn = sa.create_engine(url)

    return engn


# Create a connection to Redshift
def create_redshift_conn():
    hst, _, dbnm, usr, pwd = get_secret_creds()

    conn = redshift_connector.connect(
        host = hst,
        database = dbnm,
        user = usr,
        password = pwd
    )

    return conn


# Write one CSV chunk to local folder compressed with gzip
def write_df_to_csv(params):
    locl_fldr, file_nm, chunk_dfm = params

    chunk_dfm.to_csv(os.path.join(locl_fldr, file_nm), compression='gzip', index=False, quoting=csv.QUOTE_NONNUMERIC)

    return 0


# Upload a gzipped CSV chunk to S3
def upload_to_s3(params):
    locl_fldr, s3_bkt, file_name = params

    s3 = boto3.client('s3')
    with open(os.path.join(locl_fldr, file_name), "rb") as f:
        s3.upload_fileobj(f, s3_bkt, file_name)


# Write chunks of CSV file in parallel to local folder and upload them to S3 in parallel.
# The number of chunks should be at least as many as the number fo slices in Redshift.
# Size of file preferably between 1-125 MB
def write_df_to_csv_and_upload_to_s3_parallel(dfm, out_fil_pref, pl_siz, locl_fldr, s3_bkt):
    num_slices = 8
    total_rows = len(dfm)
    num_chunks = max(dfm.memory_usage().sum()/(128 * 1024 * 1024), num_slices)
    chunk_size = total_rows // num_chunks

    # Create lists for the chunks to be written and the gzipped files to be uploaded to S3
    chunk_start = 0
    chunk_end = chunk_start + chunk_size
    chunk_list = []
    upload_list = []
    chunk_num = 0

    while chunk_end < total_rows:
        if chunk_end > total_rows:
            chunk_end = total_rows

        file_name = out_fil_pref + '_' + str(chunk_num) + '.gz'
        chunk_list.append((locl_fldr, file_name, dfm.iloc[chunk_start:chunk_end]))
        upload_list.append((locl_fldr, s3_bkt, file_name))

        chunk_start = chunk_end
        chunk_end += chunk_size
        chunk_num += 1

    # Clean up local folder of any previously generated files
    shutil.rmtree(locl_fldr, ignore_errors=True)
    os.makedirs(locl_fldr, exist_ok=True)

    # Clean up S3 folder of files from any previous loads
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bkt)
    bucket.objects.all().delete()

    p = multiprocessing.Pool(pl_siz)

    # Write CSV chunks to local folder in parallel
    _ = p.map(write_df_to_csv, chunk_list)

    # Upload CSV chunks from local folder to S3 in parallel
    _ = p.map(upload_to_s3, upload_list)
    p.close()
    p.join()

    shutil.rmtree(locl_fldr, ignore_errors=True)

    return upload_list


# Delete the table created by to_sql to remove the one row used to create the table
def delete_table(schm_nm, tbl_nm):
    conn = create_redshift_conn()

    conn.cursor().execute(f"DELETE FROM {schm_nm}.{tbl_nm}")

    conn.commit()
    conn.close()

    return 0


# Load the files uploaded to S3 to the Redshift table. This will do a parallel
# load as we are providing a prefix to the COPY command. A manifest file could
# be used instead
def load_data_to_redshift_parallel(s3_bkt, file_prefix, schm_nm, tbl_nm):
    iam_role = get_secret_iam_role().strip()

    conn = create_redshift_conn()

    # For the small amount of data in this sample, the Redshift parallel load time isn't all that
    # different whether there is one file (loaded by one slice) or multiple files (loaded by multiple
    # slices in parallel), but for large data sets, it will make a large difference
    conn.cursor().execute(f"""COPY {schm_nm}.{tbl_nm}
                              FROM 's3://{s3_bkt}/{file_prefix}'
                              IAM_ROLE '{iam_role}'
                              FORMAT CSV
                              IGNOREHEADER 1
                              GZIP""")

    conn.commit()
    conn.close()

    return 0


if __name__ == "__main__":
    local_folder = '<<Your local folder>>'
    s3_bucket = '<<Your S3 bucket>>'
    # Place the sample dataset in some folder as the starting point
    file_path = os.path.join('/tmp', 'trade_transactions.csv')
    schema_name = '<<Your schema name>>'
    table_name = 'trade_transactions'
    out_file_prefix = 'trade_trans'
    pool_size = 8
    start_time = datetime.now()

    try:
        # Read the source data from CSV
        df = pd.read_csv(file_path, quoting=csv.QUOTE_NONNUMERIC)

        engine = get_engine(*get_secret_creds())

        # Do not do this! Takes 21 minutes to load the data to table even for this relatively small dataset
        # df.to_sql(name=table_name, schema=schema_name, con=engine, index=False, if_exists='replace',
        #           method='multi', chunksize=4000)

        # Do this instead. The load now takes only 45 seconds!
        # Create a single row dataframe from df
        df_one_row = df[0:1]

        # Create the table using the one row dataframe
        df_one_row.to_sql(name=table_name, schema=schema_name, con=engine, index=False, if_exists='replace')
        # Delete the one row used to create the table. We will write the whole dataset including that row
        delete_table(schema_name, table_name)

        # Write chunks of CSV file in parallel to local folder and upload them to S3 in parallel
        write_df_to_csv_and_upload_to_s3_parallel(df, out_file_prefix, pool_size, local_folder, s3_bucket)

        # Load the files uploaded to S3 to the Redshift table
        load_data_to_redshift_parallel(s3_bucket, out_file_prefix, schema_name, table_name)
    except Exception as ex:
        print(ex)

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    print(f'Time to write table: {elapsed_time}')

    exit(0)

