{{
    config(
        materialized='table'
    )
}}

with fact_trips as (
    select * from {{ ref('fact_trips') }}
    where fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit Card')
),
dim_trip_date as (
    select * from {{ ref('dim_taxi_trips_date') }}
),
fare_perc as (
    select
        t.service_type,
        d.pu_year,
        d.pu_month,
        percentile_cont(fare_amount, 0.90) over (
            partition by service_type, pu_year, pu_month
        ) as fare_p90,
        percentile_cont(fare_amount, 0.95) over (
            partition by service_type, pu_year, pu_month
        ) as fare_p95,
        percentile_cont(fare_amount, 0.97) over (
            partition by service_type, pu_year, pu_month
        ) as fare_p97
    from fact_trips as t
    join dim_trip_date as d
        on t.pickup_datetime = d.pickup_datetime
)
select distinct
    service_type,
    pu_year,
    pu_month,
    fare_p90,
    fare_p95,
    fare_p97
from fare_perc