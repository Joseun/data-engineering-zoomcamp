#!/usr/bin/env python
""" MODULE TO TRANSFER DATA FROM GCS TO BQ"""

import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def extract_from_gcs(year: int, nickname: str) -> None:
    """ Extract data from GCS to local"""
    
    filepath = f"data/nba/season{year}/{nickname}.parquet"
    
    bucket = GcsBucket.load("zoom-gcs")
    bucket.get_directory(
        from_path=filepath,
        local_path="data/"
    )
    return Path(f"{filepath}")

@task()
def write_bq(df: pd.DataFrame, nickname: str) -> None:
    """ Write DataFrame to Big Query """

    creds = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table=f"nbaplayers.{nickname}",
        project_id="divine-catalyst-375310",
        credentials=creds.get_credentials_from_service_account(),
        if_exists="replace"
    )
    print(f" Success: {nickname} table for {year} NBA season created")


@flow(name="GCS to BQ")
def etl_gcs_to_bq(year: int = 2022):
    """ Main EL flow from GCS to BQ"""
    data = pd.read_parquet(r"/home/joseun/data-engineering-zoomcamp/cohorts/2023/week_7_project/data/nba/teams_lookup.parquet")
    club_nick = [i.strip("\"") for i in data['nickname'].to_list()]
    for nickname in club_nick:
        path = extract_from_gcs(year, nickname)
        df = pd.read_parquet(path)
        write_bq(df, nickname)

if __name__ == '__main__':
    year = 2022
    etl_gcs_to_bq(year)
