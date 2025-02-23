{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
dim_trip_date as (
    select * from {{ ref('dim_taxi_trips_date') }}
),
quarterly_rev as (
    select 
        -- Revenue grouping 
        trips_data.service_type, 
        dim_trip_date.pu_year_quarter,
        dim_trip_date.pu_year,
        dim_trip_date.pu_quarter,
        -- Revenue calculation 
        sum(total_amount) as total_revenue_quarter
    from trips_data
    join dim_trip_date
    on trips_data.pickup_datetime = dim_trip_date.pickup_datetime
    where dim_trip_date.pu_year in (2019, 2020)
    group by 1,2,3,4
),
revenue_with_lag as (
    select 
        curr.service_type,
        curr.pu_year,
        curr.pu_year_quarter,
        curr.total_revenue_quarter,
        
        -- Get the previous year's revenue for the same quarter
        past.total_revenue_quarter as prev_year_revenue

    from quarterly_rev as curr
    left join quarterly_rev as past
        on curr.service_type = past.service_type
        and curr.pu_year = past.pu_year + 1
        and curr.pu_quarter = past.pu_quarter
)

select 
    service_type,
    pu_year,
    pu_year_quarter,
    total_revenue_quarter,
    prev_year_revenue,
    
    -- Calculate YoY % Change
    case 
        when prev_year_revenue is not null and prev_year_revenue != 0 
        then ((total_revenue_quarter - prev_year_revenue) / prev_year_revenue) * 100
        else null
    end as yoy_revenue_change_pct
from revenue_with_lag
order by yoy_revenue_change_pct desc