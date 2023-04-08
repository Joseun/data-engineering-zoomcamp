
  
    

    create or replace table `divine-catalyst-375310`.`dbt_nba`.`fact_players`
    
    
    OPTIONS()
    as (
      

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

    from `divine-catalyst-375310`.`dbt_nba`.`stg_76ers_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_bucks_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_bulls_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_blazers_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_cavaliers_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_celtics_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_clippers_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_grizzlies_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_hawks_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_heat_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_hornets_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_jazz_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_kings_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_knicks_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_lakers_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_magic_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_mavericks_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_nets_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_nuggets_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_pacers_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_pelicans_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_pistons_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_raptors_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_rockets_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_spurs_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_suns_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_thunder_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_timberwolves_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_warriors_players`
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
    from `divine-catalyst-375310`.`dbt_nba`.`stg_wizards_players`
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
    case
        WHEN BMI < 16.5 THEN 'Severely underweight'
        WHEN BMI >= 16.5 AND BMI < 18.5 THEN 'Underweight'
        WHEN BMI >= 18.5 AND BMI < 25 THEN 'Normal weight'
        WHEN BMI >= 25 AND BMI < 30 THEN 'Overweight'
        WHEN BMI >= 30 AND BMI < 35 THEN 'Class I Obesity'
        WHEN BMI >= 35 AND BMI < 40 THEN 'Class II Obesity'
        ELSE 'Class III Obesity'
    end as bmi_class,
    nba_players_stat.college,
    nba_players_stat.jersey_number,
    nba_players_stat.position,
from nba_players_stat
    );
  