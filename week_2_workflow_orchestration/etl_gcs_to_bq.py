#!/usr/bin/env python
# coding: utf-8
import argparse
import pandas as pd

from datetime import timedelta
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def extract_from_gcs(params) -> None:
    """ The main ETL function t load data into Big Query """
    color = params.color
    year = params.year
    month = params.month
    
    filepath = f"{color}/{color}_tripdata_{year}-{month:0>2}.parquet"
    
    bucket = GcsBucket.load("zoom-gcs")
    bucket.get_directory(
        from_path=filepath,
        local_path="data/"
    )
    return Path(f"data/{filepath}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """ Data Cleaning function """
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """ Write DataFrame to Big Query """

    creds = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="divine-catalyst-375310",
        credentials=creds.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(name="GCS to BQ")
def etl_gcs_to_bq():
    parser = argparse.ArgumentParser(description='Ingest CSV data to GCS')
    parser.add_argument('--color', required=True, help='name of the color of the taxi')
    parser.add_argument('--year', required=True, help='year of the data required')
    parser.add_argument('--month', required=True, help='month of the data required')

    args = parser.parse_args()
    path = extract_from_gcs(args)
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()