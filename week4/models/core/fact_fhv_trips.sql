{{ config(materialized="table") }}


with
    dropoff_zone as (
        select locationid, borough as dropoff_borough, zone as dropoff_zone
        from {{ ref("dim_zones") }}
        where borough != 'Unknown'
    ),
    pickup_zone as (
        select locationid, borough as pickup_borough, zone as pickup_zone
        from {{ ref("dim_zones") }}
        where borough != 'Unknown'
    )
select
    tripid,
    dispatching_base_num,
    sr_flag,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
from {{ ref("stg_fhv_tripdata") }}
inner join pickup_zone on stg_fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dropoff_zone on stg_fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
