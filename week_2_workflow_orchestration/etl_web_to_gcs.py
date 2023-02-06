#!/usr/bin/env python
# coding: utf-8
import argparse
import pandas as pd

from datetime import timedelta
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(data_url: str) -> pd.DataFrame:
    """ Read taxi data from web into pandas DataFrame """

    df = pd.read_csv(data_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, color) -> pd.DataFrame:
    """ Fix dtype issues """
    if color == "yellow":
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    elif color == "green":
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, file_name: str) -> Path:
    """ Write DataFrame out locally as a parquet file """
    path = Path(f"data/{color}/{file_name}.parquet")
    path.parents[0].mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """ Upload local parquet file to GCS """
    bucket = GcsBucket.load("zoom-gcs")
    bucket.upload_from_path(
        from_path=path,
        to_path=path
    )
    return

@flow(name="SubFlow")
def etl_web_to_gcs(params) -> None:
    """ The main ETL function """
    color = params.color
    year = params.year
    month = params.month
    
    file_name = f"{color}_tripdata_{year}-{month:0>2}"
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}.csv.gz"

    raw_data = fetch(data_url)
    cleaned_data = clean(raw_data, color)
    path = write_local(cleaned_data, color, file_name)
    write_gcs(path)

@flow(name="Web to GCS")
def main():
    parser = argparse.ArgumentParser(description='Ingest CSV data to GCS')
    parser.add_argument('--color', required=True, help='name of the color of the taxi')
    parser.add_argument('--year', required=True, help='year of the data required')
    parser.add_argument('--month', required=True, help='month of the data required')

    args = parser.parse_args()
    etl_web_to_gcs(args)

if __name__ == '__main__':
    main()