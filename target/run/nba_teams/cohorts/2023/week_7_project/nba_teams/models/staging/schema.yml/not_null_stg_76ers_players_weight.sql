select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select weight
from `divine-catalyst-375310`.`dbt_nba`.`stg_76ers_players`
where weight is null



      
    ) dbt_internal_test