{{ config(materialized='view') }}


select
   -- identifiers
    cast(int64_field_0 as integer) as tripid,
    dispatching_base_num,
    SR_Flag, -- ? not null
    -- Affiliated_base_number,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
from {{ source('staging','fhv') }}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
