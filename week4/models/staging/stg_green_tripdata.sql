{{ config(materialized="view") }}

select * from {{ source('staging', 'green') }}
limit 100


-- select * from  dez_ny_taxi.green limit 100