

  create or replace view `divine-catalyst-375310`.`dbt_nba`.`stg_timberwolves_players`
  OPTIONS()
  as 


select
  CAST(name as string) as name,
  CAST(REPLACE(pob, "\"", "") as string) as place_of_birth,
  EXTRACT(YEAR FROM (PARSE_TIMESTAMP('%Y', debut_year))) - EXTRACT(YEAR FROM (dob)) as debut_age,

  CAST(dob as timestamp) as date_of_birth,
  FORMAT_DATE('%B', DATE_TRUNC(dob, MONTH)) as month_of_birth,
  FORMAT_DATE('%A', DATE_TRUNC(dob, MONTH)) as day_of_birth,
  PARSE_TIMESTAMP('%Y', debut_year) as debut_year,
  
  CAST(REPLACE(height, "\"", "") AS FLOAT64) as height,
  CAST(REPLACE(weight, "\"", "") AS FLOAT64) as weight,
  CAST(REPLACE(college, "\"", "") as string) as college,
  CAST(REPLACE(affiliation, "\"", "") as string) as affiliation,
  CAST(jersey as integer) as jersey_number,
  CAST(REPLACE(position, "\"", "") as string) as position,

from `divine-catalyst-375310`.`nbaplayers`.`Timberwolves`
where jersey is not null and LENGTH(debut_year) = 4

-- dbt build --m <model.sql> --var 'is_test_run: false'


  limit 100

;

