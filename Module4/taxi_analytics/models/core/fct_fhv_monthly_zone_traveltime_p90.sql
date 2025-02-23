{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select * from {{ ref('dim_fhv_trips') }}
),

duration as (
    select
        pickup_datetime,
        pickup_location_id,
        pickup_zone,
        dropoff_location_id,
        dropoff_zone,
        year,
        month,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from tripdata
),

p90_calc as (
    select
        pickup_datetime,
        pickup_location_id,
        pickup_zone,
        dropoff_location_id,
        dropoff_zone,
        year,
        month,
        percentile_cont(trip_duration, 0.90) over (
            partition by year, month, pickup_location_id, dropoff_location_id
        ) as p90
    from duration
)
select distinct * from p90_calc
