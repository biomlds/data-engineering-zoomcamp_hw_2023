select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select tripid
from `hip-watch-375918`.`dez_ny_taxi`.`stg_green_tripdata`
where tripid is null



      
    ) dbt_internal_test