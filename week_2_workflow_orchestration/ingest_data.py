#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd

from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from time import time

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {csv_url} -O {csv_name}")

    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def ingest_data(params, df):
    table_name = params.table_name

    conn = SqlAlchemyConnector.load("postgres-connector")

    with conn.get_connection(begin=False) as engine:
        # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')


    # while True: 

    #     try:
    #         t_start = time()
            
    #         df = next(df_iter)

    #         df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #         df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break

@flow(name="Ingest Flow")
def main():
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()
    raw_data = extract_data(args.url)
    processed_data = transform_data(raw_data)
    ingest_data(args, processed_data)

if __name__ == '__main__':
    main()
