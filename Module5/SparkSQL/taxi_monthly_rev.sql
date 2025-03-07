SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    --SUM(ehail_fee) AS revenue_monthly_ehail_fee, -- removed only in green
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    --COUNT(trip_id) AS total_monthly_trips, --did not calculate tripid
    AVG(passenger_count) AS _monthly_passenger_count,
    AVG(trip_distance) AS _monthly_trip_distance

    FROM trips_data
    GROUP BY revenue_zone,revenue_month,service_type