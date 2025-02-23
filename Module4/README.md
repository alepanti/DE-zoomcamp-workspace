# Module 4 Homework

1.
```
select * from myproject.raw_nyc_tripdata.ext_green_taxi
```

2.
Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

3.
```
dbt run --select models/staging/+
```

4.
NOT TRUE: Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile

5.
green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

6.
```
SELECT * FROM `zoomcamp-dwh-450019.dbt_apantig.fct_taxi_trips_monthly_fare_p95` 
WHERE pu_month = 4 and pu_year = 2020
```
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

7.
```
WITH ranked AS (
    SELECT 
        pickup_zone,
        dropoff_zone,
        p90,
        DENSE_RANK() OVER (
            PARTITION BY pickup_zone 
            ORDER BY p90 DESC
        ) AS rank
    FROM `zoomcamp-dwh-450019.dbt_apantig.fct_fhv_monthly_zone_traveltime_p90`
    WHERE 
        pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND year = 2019
        AND month = 11
)

SELECT pickup_zone, dropoff_zone, p90
FROM ranked
WHERE rank = 2;
```
LaGuardia Airport, Chinatown, Garment District
