
    
    

with dbt_test__target as (

  select name as unique_field
  from `divine-catalyst-375310`.`dbt_nba`.`stg_76ers_players`
  where name is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


