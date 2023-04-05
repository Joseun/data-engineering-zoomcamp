#!/usr/bin/env python
# coding: utf-8

import time
import pandas as pd
import numpy as np 
import json
from datetime import timedelta
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from decouple import config
import requests

# players_table = "cloud-data-infrastructure.football_data_dataset.players"

# def gcp_secret():
#     # Import the Secret Manager client library.
#     from google.cloud import secretmanager

#     # Create the Secret Manager client.
#     client = secretmanager.SecretManagerServiceClient()

#     # Build the resource name of the secret version.
#     name = "projects/463690670206/secrets/rapid-api/versions/1"

#     # Access the secret version.
#     response = client.access_secret_version(request={"name": name})

#     payload = response.payload.data.decode("UTF-8")
#     return payload

@task(log_prints=True, retries=3)
def fetch() -> json:
	# Headers used for RapidAPI.-
	headers = {
		"X-RapidAPI-Key": config("KEY"),
		"X-RapidAPI-Host": "api-nba-v1.p.rapidapi.com"
	}
	# Standings endpoint from RapidAPI.
	url = "https://api-nba-v1.p.rapidapi.com/teams"
	# Building query to retrieve data.
	try:
		response = requests.request("GET", url, headers=headers)
		json_res = response.json()
	except Exception as e:
		print(e)
		json_res = None
	return json_res

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_teams(json_res:str):
	# Empty lists that will be filled and then used to create a dataframe.
	id_list = []
	name = []
	nickname = []
	code = []
	city = []
	nbafranchise = []
	allstar = []


	count = 0
	while count < len(json_res["response"]):

		# Retrieving player's first and last name then combining for full name.
		id_list.append(int(json.dumps(json_res["response"][count]["id"], ensure_ascii=False)))
		name.append(str(json.dumps(json_res["response"][count]["name"], ensure_ascii=False)))

		# Retrieving amount of goals per player.
		nickname.append(str(json.dumps(json_res["response"][count]["nickname"])))
		code.append(str(json.dumps(json_res["response"][count]["code"])))
		city.append(str(json.dumps(json_res["response"][count]["city"])))
		nbafranchise.append(str(json.dumps(json_res["response"][count]["nbaFranchise"])))
		allstar.append(str(json.dumps(json_res["response"][count]["allStar"])))
		
		count += 1

	# Setting the headers then zipping the lists to create a dataframe.
	headers = ['club_id', 'name', 'nickname', 'code', 'city', 'allstar', 'nbafranchise']
	zipped = list(zip(id_list, name, nickname, code, city, allstar, nbafranchise))

	df = pd.DataFrame(zipped, columns = headers)

	return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
	""" Fix dtype issues """
	df.replace('null', np.nan, inplace=True)
	print(df.head(2))
	print(df.isnull().sum())
	df.dropna(inplace=True)
	print(df.isnull().sum())
	df = df.loc[(df['nbafranchise']=='true') & (df['allstar']=='false')]
	df.convert_dtypes()
	print(f"Cleaned columns: {df.dtypes}")
	print(f"rows: {len(df)}")
	return df

@task(log_prints=True)
def write_local(df: pd.DataFrame) -> Path:
	""" Write DataFrame out locally as a parquet file """
	path = Path(f"data/nba/teams_lookup.parquet")
	path.parents[0].mkdir(parents=True, exist_ok=True)
	df.to_parquet(path, compression="gzip")
	print(f"Data saved to {path}")
	return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
	""" Upload local parquet file to GCS """
	bucket = GcsBucket.load("zoom-gcs")
	bucket.upload_from_path(
		from_path=path,
		to_path=path
	)
	print(f"Local data uploaded to {path} in Cloud Storage")
	return

@flow(log_prints=True)
def etl_teams_flow():
	# print(years)
	# if years == ["*"]:
	try:
		json_res = fetch()
		df = get_teams(json_res)
		print(f"Success: Teams in the NBA extracted")
		cleaned_df = clean(df.drop_duplicates())
		print(f"Success: Teams in the NBA cleaned")
		path = write_local(cleaned_df)
		write_gcs(path)
		print(f" Success: {len(cleaned_df)} rows uploaded to GCS")
	except Exception as e:
		print(e)
		print(json_res['results'])
		print(f"Error: Problem with loading data for NBA teams to GCS")


if __name__ == '__main__':
	etl_teams_flow()
