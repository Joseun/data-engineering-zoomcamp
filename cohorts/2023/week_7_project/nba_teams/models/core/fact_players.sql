{{ config(materialized='table') }}

with 76ers_players_stat as (
    select 
        place_of_birth,
        day_of_birth,
        month_of_birth,
        debut_age,
        height,
        weight,
        (weight /(height * height)) as BMI,
        college,
        jersey_number,
        position,

    from {{ ref('stg_76ers_players') }}
), 

with bulls_players_stat as (
    select
        place_of_birth,
        day_of_birth,
        month_of_birth,
        debut_age,
        height,
        weight,
        (weight /(height * height)) as BMI,
        college,
        jersey_number,
        position, 
    from {{ ref('stg_bulls_players') }}
), 

nba_players_stat as (
    select * from 76ers_players_stat
    union all
    select * from bulls_players_stat
), 

select 
    nba_players_stat.place_of_birth,
    nba_players_stat.day_of_birth,
    nba_players_stat.month_of_birth,
    nba_players_stat.debut_age,
    nba_players_stat.height,
    nba_players_stat.weight,
    nba_players_stat.BMI,
    {{ get_bmi_class('BMI') }} as bmi_class
    nba_players_stat.college,
    nba_players_stat.jersey_number,
    nba_players_stat.position,
from nba_players_stat
