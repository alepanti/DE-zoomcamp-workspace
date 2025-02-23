{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *,
    EXTRACT(MONTH FROM pickup_datetime) as month,
    EXTRACT(YEAR FROM pickup_datetime) as year,
    from {{ ref('stg_fhv') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    f.dispatching_base_num,
    f.pickup_datetime,
    f.month,
    f.year,
    f.dropoff_datetime,
    f.affiliated_base_number,
    f.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    f.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata as f
inner join dim_zones as pickup_zone
on f.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on f.dropoff_location_id = dropoff_zone.location_id