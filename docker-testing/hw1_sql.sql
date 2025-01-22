
# q1 24.3.1
# q2 postgres:5432

# q3
# 104802 198924 109603 27678 35189
SELECT COUNT(1)
FROM green_taxi_data
WHERE trip_distance <= 1

SELECT max(trip_distance)
FROM green_taxi_data
WHERE CAST(green_taxi_data.lpep_pickup_datetime AS DATE) >= '10-01-2019'::date


with t (id, low, high) as
(
 values
 ('interval-A', -10, 1),
 ('interval-B', 1, 3),
 ('interval-C', 3, 7),
 ('interval-D', 7, 10),
 ('interval-E', 10, 520)
)
select t.id, count(l.trip_distance) as "count"
from t
cross join lateral 
(
 select trip_distance from green_taxi_data g 
 where g.trip_distance > t.low AND g.trip_distance <= t.high
 AND g.lpep_pickup_datetime >= '10-01-2019'::date AND g.lpep_pickup_datetime < '11-01-2019'::date
 AND g.lpep_dropoff_datetime >= '10-01-2019'::date AND g.lpep_dropoff_datetime < '11-01-2019'::date
) l
group by t.id;


SELECT
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS up_to_1_mile,
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS between_1_and_3_miles,
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS between_3_and_7_miles,
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS between_7_and_10_miles,
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS over_10_miles
FROM
    green_taxi_data
WHERE
    lpep_pickup_datetime >= '2019-10-01' AND lpep_pickup_datetime < '2019-11-01'
	AND
	lpep_dropoff_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01';

# q4
# 2019-10-31
SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_data
ORDER BY trip_distance DESC

# q5
# East Harlem North, East Harlem South, Morningside Heights
SELECT 
z."Zone"
, SUM(g.total_amount) AS total
FROM green_taxi_data g
JOIN taxi_lookup_zones z on g."PULocationID" = z."LocationID"
WHERE CAST(g.lpep_pickup_datetime AS DATE)  = '2019-10-18'
GROUP BY z."Zone"
HAVING SUM(g.total_amount) > 13000

# q6
# JFK Airport
SELECT g.lpep_pickup_datetime,
pz."Zone",
dz."Zone",
g.tip_amount
FROM green_taxi_data g 
JOIN taxi_lookup_zones pz ON g."PULocationID" = pz."LocationID"
JOIN taxi_lookup_zones dz ON g."DOLocationID" = dz."LocationID"
WHERE pz."Zone" = 'East Harlem North' 
AND CAST(g.lpep_pickup_datetime AS DATE) >= '2019-10-01'
AND CAST(g.lpep_pickup_datetime AS DATE) < '2019-11-01'
ORDER BY g.tip_amount DESC