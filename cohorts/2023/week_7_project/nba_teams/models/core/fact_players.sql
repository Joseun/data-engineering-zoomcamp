{{ config(materialized='table') }}

with sixers_players_stat as (
    select
        name,
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

bucks_players_stat as (
    select
        name,
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
    from {{ ref('stg_bucks_players') }}
),

bulls_players_stat as (
    select
        name,
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

blazers_players_stat as (
    select
        name,
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
    from {{ ref('stg_blazers_players') }}
),

cavaliers_players_stat as (
    select
        name,
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
    from {{ ref('stg_cavaliers_players') }}
),

celtics_players_stat as (
    select
        name,
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
    from {{ ref('stg_celtics_players') }}
), 

clippers_players_stat as (
    select
        name,
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
    from {{ ref('stg_clippers_players') }}
), 

grizzlies_players_stat as (
    select
        name,
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
    from {{ ref('stg_grizzlies_players') }}
), 

hawks_players_stat as (
    select
        name,
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
    from {{ ref('stg_hawks_players') }}
), 

heat_players_stat as (
    select
        name,
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
    from {{ ref('stg_heat_players') }}
), 

hornets_players_stat as (
    select
        name,
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
    from {{ ref('stg_hornets_players') }}
), 

jazz_players_stat as (
    select
        name,
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
    from {{ ref('stg_jazz_players') }}
), 

kings_players_stat as (
    select
        name,
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
    from {{ ref('stg_kings_players') }}
), 

knicks_players_stat as (
    select
        name,
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
    from {{ ref('stg_knicks_players') }}
), 

lakers_players_stat as (
    select
        name,
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
    from {{ ref('stg_lakers_players') }}
), 

magic_players_stat as (
    select
        name,
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
    from {{ ref('stg_magic_players') }}
), 

mavericks_players_stat as (
    select
        name,
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
    from {{ ref('stg_mavericks_players') }}
), 

nets_players_stat as (
    select
        name,
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
    from {{ ref('stg_nets_players') }}
), 

nuggets_players_stat as (
    select
        name,
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
    from {{ ref('stg_nuggets_players') }}
), 

pacers_players_stat as (
    select
        name,
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
    from {{ ref('stg_pacers_players') }}
), 

pelicans_players_stat as (
    select
        name,
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
    from {{ ref('stg_pelicans_players') }}
), 

pistons_players_stat as (
    select
        name,
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
    from {{ ref('stg_pistons_players') }}
), 

raptors_players_stat as (
    select
        name,
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
    from {{ ref('stg_raptors_players') }}
), 

rockets_players_stat as (
    select
        name,
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
    from {{ ref('stg_rockets_players') }}
), 

spurs_players_stat as (
    select
        name,
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
    from {{ ref('stg_spurs_players') }}
), 

suns_players_stat as (
    select
        name,
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
    from {{ ref('stg_suns_players') }}
), 

thunder_players_stat as (
    select
        name,
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
    from {{ ref('stg_thunder_players') }}
), 

timberwolves_players_stat as (
    select
        name,
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
    from {{ ref('stg_timberwolves_players') }}
), 

warriors_players_stat as (
    select
        name,
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
    from {{ ref('stg_warriors_players') }}
), 

wizards_players_stat as (
    select
        name,
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
    from {{ ref('stg_wizards_players') }}
), 

nba_players_stat as (
    select
        * from sixers_players_stat
    union all
    select
        * from bucks_players_stat
    union all
    select
        * from blazers_players_stat
    union all
    select
        * from bulls_players_stat
    union all
    select
        * from cavaliers_players_stat
    union all
    select
        * from celtics_players_stat
    union all
    select
        * from clippers_players_stat
    union all
    select
        * from grizzlies_players_stat
    union all
    select
        * from hawks_players_stat
    union all
    select
        * from hawks_players_stat
    union all
    select
        * from heat_players_stat
    union all
    select
        * from hornets_players_stat
    union all
    select
        * from jazz_players_stat
    union all
    select
        * from kings_players_stat
    union all
    select
        * from knicks_players_stat
    union all
    select
        * from lakers_players_stat
    union all
    select
        * from magic_players_stat
    union all
    select
        * from mavericks_players_stat
    union all
    select
        * from nets_players_stat
    union all
    select
        * from nuggets_players_stat
    union all
    select
        * from pacers_players_stat
    union all
    select
        * from pelicans_players_stat
    union all
    select
        * from pistons_players_stat
    union all
    select
        * from raptors_players_stat
    union all
    select
        * from rockets_players_stat
    union all
    select
        * from spurs_players_stat
    union all
    select
        * from suns_players_stat
    union all
    select
        * from thunder_players_stat
    union all
    select
        * from timberwolves_players_stat
    union all
    select
        * from warriors_players_stat
    union all
    select
        * from wizards_players_stat
) 

select
    DISTINCT nba_players_stat.name,
    nba_players_stat.place_of_birth,
    nba_players_stat.day_of_birth,
    nba_players_stat.month_of_birth,
    nba_players_stat.debut_age,
    nba_players_stat.height,
    nba_players_stat.weight,
    nba_players_stat.BMI,
    {{ get_bmi_class('BMI') }} as bmi_class,
    nba_players_stat.college,
    nba_players_stat.jersey_number,
    nba_players_stat.position,
from nba_players_stat
