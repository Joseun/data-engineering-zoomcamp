#!/usr/bin/env python
""" MODULE TO EXTRACT DATA FROM API AND LOAD TO GCS """

import json
import numpy as np 
import pandas as pd
import requests
import time

from datetime import timedelta
from decouple import config
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket



@task(log_prints=True, retries=3)
def fetch(year: int, team: int) -> json:
	""" Fetchs json object via API call """
	headers = {
		"X-RapidAPI-Key": config("KEY"),
		"X-RapidAPI-Host": "api-nba-v1.p.rapidapi.com"
	}
	# Players endpoint from RapidAPI.
	url = "https://api-nba-v1.p.rapidapi.com/players"

	# Building query to retrieve data.
	query = {
		"season":f"{year}",
		"team":f"{team}"
		}
	try:
		response = requests.request("GET", url, headers=headers, params=query)
		json_res = response.json()
	except Exception as e:
		print(e)
		json_res = None
	return json_res

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_players(json_res:str) -> pd.DataFrame:
	""" Create a dataset from json response """
	# Empty lists that will be filled and then used to create a dataframe.
	full_name = []
	birth_date = []
	birth_country = []
	pro_age = []
	start_pro = []
	height = []
	weight = []
	college_list = []
	affiliation_list = []
	jersey_number = []
	position = []

	count = 0

	while count < len(json_res["response"]):

		# Retrieving player's first and last name then combining for full name.
		first_name = (str(json.dumps(json_res["response"][count]["firstname"], ensure_ascii=False))).strip('"')
		last_name = (str(json.dumps(json_res["response"][count]["lastname"], ensure_ascii=False))).strip('"')

		name = first_name + " " + last_name
		
		full_name.append(name)

		birth_date.append(str(json.dumps(json_res["response"][count]["birth"]["date"])))
		birth_country.append(str(json.dumps(json_res["response"][count]["birth"]["country"])))
		pro_age.append(str(json.dumps(json_res["response"][count]["nba"]["pro"])))
		start_pro.append(str(json.dumps(json_res["response"][count]["nba"]["start"])))
		height.append(((str(json.dumps(json_res["response"][count]["height"]["meters"])))))
		weight.append(((str(json.dumps(json_res["response"][count]["weight"]["kilograms"])))))
		college_list.append((str(json.dumps(json_res["response"][count]["college"]))))
		affiliation_list.append((str(json.dumps(json_res["response"][count]["affiliation"]))))

		try:
			jersey_number.append(str(json.dumps(json_res["response"][count]["leagues"]["standard"]["jersey"])))
		except:
			jersey_number.append('null')
		try:
			position.append(str(json.dumps(json_res["response"][count]["leagues"]["standard"]["pos"])))
		except:
			position.append('null')
		count += 1
		

	# Setting the headers then zipping the lists to create a dataframe.
	headers = ['name', 'dob', 'pob', 'debut_age', 'debut_year', 'height', 'weight', 'college', 'affiliation', 'jersey', 'position']
	zipped = list(zip(full_name, birth_date, birth_country, pro_age, start_pro, height, weight, college_list, affiliation_list, jersey_number, position))

	df = pd.DataFrame(zipped, columns = headers)

	return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
	""" Cleans dataframe  """
	df.replace('null', np.nan, inplace=True)
	print(df.head(2))
	print(df.isnull().sum())
	df.dropna(subset=['dob', 'debut_year','height', 'weight'], inplace=True)
	print(df.duplicated().sum())
	df.drop_duplicates(inplace=True)
	df.dob = pd.to_datetime(df['dob'].str.strip("\""), format="%Y-%m-%d")
	df.convert_dtypes()
	print(f"Cleaned columns: {df.dtypes}")
	rows = len(df)
	print(f"rows: {rows}")
	return df, rows

@task(log_prints=True)
def write_local(df: pd.DataFrame, year: int, nickname: str) -> Path:
	""" Write DataFrame out locally as a parquet file """
	path = Path(f"data/nba/season{year}/{nickname}.parquet")
	path.parents[0].mkdir(parents=True, exist_ok=True)
	df.to_parquet(path, compression="gzip")
	print(f"Saved to {path}")
	return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
	""" Upload local parquet file to GCS """
	bucket = GcsBucket.load("zoom-gcs")
	bucket.upload_from_path(
		from_path=path,
		to_path=path
	)
	print(f"Loaded data from {path} in local to {path} in Cloud Storage")
	return

@flow(log_prints=True)
def etl_players_flow(year: int = 2022):
	""" Main ETL flow """
	try:
		data = pd.read_parquet(r"/home/joseun/data-engineering-zoomcamp/cohorts/2023/week_7_project/data/nba/teams_lookup.parquet")
		club_id = data['club_id'].to_list()
		club_name = [i.strip("\"") for i in data['name'].to_list()]
		club_nick = [i.strip("\"") for i in data['nickname'].to_list()]
		count = 1
		total_rows = 0
		
		for id, name, nickname in zip(club_id, club_name, club_nick): # There are 63 unique IDs tied to teams from the NBA api 
			json_res = fetch(year, id)
			if json_res['response']:
				print(f" Success: {name} team for {year} NBA season extracted")
				df = get_players(json_res)
				cleaned_df, rows = clean(df)
				total_rows += rows
				print(f" Success: {name} team for {year} NBA season cleaned")
				path = write_local(cleaned_df, year, nickname)
				write_gcs(path)
				print(f" Success: {rows} rows for {name} team for {year} NBA season uploaded to GCS")
				print(f"Team {count} done")
				print("Sleeping for 30 seconds due to API subcription rate limit")
				time.sleep(30)
				print("Now continuing")
				count += 1
			else:
				print(json_res)
				continue
		print(f" Success: {total_rows} rows for {year} NBA season ETL to GCS")

	except Exception as e:
		print(e)
		print(f"Error: Problem with loading data for {year} NBA season to GCS")


if __name__ == '__main__':
	year = 2022
	etl_players_flow(year)
