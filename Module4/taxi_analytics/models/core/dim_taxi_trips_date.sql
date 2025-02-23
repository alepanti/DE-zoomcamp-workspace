{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select DISTINCT(pickup_datetime) as pickup_datetime from {{ ref('fact_trips') }}
)
select
    pickup_datetime,
    EXTRACT(MONTH FROM pickup_datetime) as pu_month,
    EXTRACT(YEAR FROM pickup_datetime) as pu_year,
    {{ get_quarter("pickup_datetime") }} as pu_quarter,
    CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/', {{ get_quarter("pickup_datetime") }}) as pu_year_quarter
from trips_data

