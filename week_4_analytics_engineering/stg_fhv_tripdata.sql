{{ config(materialized='view') }}
 
with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
  where extract(year from pickup_datetime) = 2019 
)
select
   -- identifiers
    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("float")) }} as sr_flag,
    affiliated_base_number,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime
from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}