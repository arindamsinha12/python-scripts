import multiprocessing
import os
import shutil
import psycopg2
import pandas as pd
import boto3
import json
import csv
import numpy as np

from sqlalchemy import create_engine
from botocore.exceptions import ClientError
from datetime import datetime
from psycopg2.sql import SQL, Identifier, Literal


def get_secret_iam_role():
    secret_name = "IamRole"
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


def get_secret_creds():
    secret_name = "sqlworkbench!ab6848ed-d04c-4d9d-832c-7f592e2f41a7"
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
    hst = secret['dbClusterIdentifier'] + '.068297384246.' + region_name + '.redshift-serverless.amazonaws.com'
    prt = secret['port']
    dbnm = secret['dbName']
    usr = secret['username']
    pwd = secret['password']

    return hst, prt, dbnm, usr, pwd


def get_engine(hst, prt, dbnm, usr, pwd):
    engn = create_engine(f'redshift+psycopg2://{usr}:{pwd}@{hst}:{prt}/{dbnm}')

    return engn


def write_df_to_csv(params):
    locl_fldr, file_nm, chunk_dfm = params

    chunk_dfm.to_csv(os.path.join(locl_fldr, file_nm), compression='gzip', index=False, quoting=csv.QUOTE_NONNUMERIC)
    chunk_dfm.to_csv(os.path.join(locl_fldr, file_nm.replace('.gz', '.csv')), index=False, quoting=csv.QUOTE_NONNUMERIC)

    return 0


def upload_to_s3(params):
    locl_fldr, s3_bkt, file_name = params

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bkt)
    bucket.objects.all().delete()

    s3 = boto3.client('s3')
    with open(os.path.join(locl_fldr, file_name), "rb") as f:
        s3.upload_fileobj(f, s3_bkt, file_name)


def write_df_to_csv_and_upload_to_s3_parallel(dfm, out_fil_pref, pl_siz, locl_fldr, s3_bkt):
    num_slices = 8
    total_rows = len(dfm)
    num_chunks = max(dfm.memory_usage().sum()/(128 * 1024 * 1024), num_slices)
    chunk_size = total_rows // num_chunks

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

    shutil.rmtree(locl_fldr, ignore_errors=True)
    os.makedirs(locl_fldr, exist_ok=True)

    p = multiprocessing.Pool(pl_siz)

    _ = p.map(write_df_to_csv, chunk_list)

    _ = p.map(upload_to_s3, upload_list)
    p.close()
    p.join()

    # shutil.rmtree(locl_fldr, ignore_errors=True)

    return upload_list


def create_redshift_conn():
    hst, prt, dbnm, usr, pwd = get_secret_creds()

    conn = psycopg2.connect(
        host = hst,
        port = prt,
        dbname = dbnm,
        user = usr,
        password = pwd
    )

    return conn


def delete_table(schm_nm, tbl_nm):
    conn = create_redshift_conn()

    conn.cursor().execute(SQL("DELETE FROM {}.{}").format(Identifier(schm_nm), Identifier(tbl_nm)))

    conn.commit()
    conn.close()

    return 0


def load_data_to_redshift_parallel(s3_bkt, file_prefix, schm_nm, tbl_nm):
    iam_role = get_secret_iam_role().strip()

    conn = create_redshift_conn()

    # For the small amount of data in this sample, the Redshift parallel load time isn't all that
    # different whether there is one file (loaded by one slice) or multiple files (loaded by multiple
    # slices in parallel), but for large data sets, it will make a large difference
    conn.cursor().execute(SQL("""COPY {}.{}
                                 FROM {}
                                 IAM_ROLE {}
                                 FORMAT CSV
                                 IGNOREHEADER 1
                                 GZIP""").format(Identifier(schm_nm), Identifier(tbl_nm),
                                                 Literal(f's3://{s3_bkt}/{file_prefix}'),
                                                 Literal(iam_role)))

    conn.commit()
    conn.close()

    return 0


if __name__ == "__main__":
    local_folder = '/tmp/upload_files/'
    s3_bucket = 'redshift-fast-upload-files'
    file_path = os.path.join('/tmp', 'trade_transactions.csv')
    schema_name = 'dev_schema'
    table_name = 'trade_transactions'
    out_file_prefix = 'trade_trans'
    pool_size = 8

    df = pd.read_csv(file_path, quoting=csv.QUOTE_NONNUMERIC)

    engine = get_engine(*get_secret_creds())

    start_time = datetime.now()

    df_one_row = df[0:1]

    df_one_row.to_sql(name=table_name, schema=schema_name, con=engine, index=False, if_exists='replace')
    delete_table(schema_name, table_name)

    write_df_to_csv_and_upload_to_s3_parallel(df, out_file_prefix, pool_size, local_folder, s3_bucket)

    load_data_to_redshift_parallel(s3_bucket, out_file_prefix, schema_name, table_name)

    end_time = datetime.now()

    elapsed_time = end_time - start_time
    print(f'Time to write table: {elapsed_time}')

    exit(0)

