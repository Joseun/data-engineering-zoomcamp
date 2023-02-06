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
def extract_from_gcs(color: str = "green", year: int = 2020, month: int = 1) -> None:
    """ The main EL function to load data into Big Query """
   
    filepath = f"{color}/{color}_tripdata_{year}-{month:0>2}.parquet"
    
    bucket = GcsBucket.load("zoom-gcs")
    bucket.get_directory(
        from_path=filepath,
        local_path="data/"
    )
    return Path(f"data/{filepath}")

@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """ Write DataFrame to Big Query """
    table = {
        "yellow":"trips_data_all.yellow_taxi_trips",
        "green":"trips_data_all.green_taxi_trips"
    }
    creds = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table=table[color],
        project_id="divine-catalyst-375310",
        credentials=creds.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(name="SubFlow", log_prints=True)
def el_gcs_to_bq(color: str = "green", year: int = 2020, month: int = 1):
    # parser = argparse.ArgumentParser(description='Ingest CSV data to GCS')
    # parser.add_argument('--color', required=True, help='name of the color of the taxi')
    # parser.add_argument('--year', required=True, help='year of the data required')
    # parser.add_argument('--month', required=True, help='month of the data required')

    # args = parser.parse_args()
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    print(f"missing values: {df.isna().sum()}")
    print(f"rows: {len(df)}")
    rows = len(df)
    write_bq(df, color)
    return rows

@flow(log_prints=True)
def el_bq_flow(color: str = "green", year: int = 2020, months: list[int] = 1):
    total_rows = 0
    for month in months:
        rows = el_gcs_to_bq(color, year, month)
        total_rows += rows
    print(f"Total rows processed: {total_rows}")

if __name__ == '__main__':
    color = "green"
    year = 2020
    months = [1]
    el_bq_flow(color, year, months)
