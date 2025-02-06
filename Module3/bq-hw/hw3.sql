-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-dwh-450019.nytaxi.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-taxi-data/yellow_tripdata_2024-*.parquet']
);

SELECT * FROM zoomcamp-dwh-450019.nytaxi.external_yellow_tripdata_2024 limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024 AS
SELECT * FROM zoomcamp-dwh-450019.nytaxi.external_yellow_tripdata_2024;

SELECT * FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024 limit 10;


-- Q1
SELECT COUNT(1) FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024;

-- Q2
SELECT COUNT(DISTINCT PULocationID) FROM zoomcamp-dwh-450019.nytaxi.external_yellow_tripdata_2024; -- Estimate 0B
SELECT COUNT(DISTINCT PULocationID) FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024; -- Estimate 155.12MB

-- Q3
SELECT PULocationID FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024;
SELECT PULocationID, DOLocationID FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024;
  -- Estimated number of bytes read differ because BQ stores data in columnar storage.

-- Q4
SELECT COUNT(1) FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024
WHERE fare_amount = 0; -- 8333

-- Q5
CREATE OR REPLACE TABLE zoomcamp-dwh-450019.nytaxi.yellow_tripdata_partitioned_clustered_2024
PARTITION BY DATE(tpep_dropoff_datetime)  -- partition on dropoff date/time
CLUSTER BY VendorID  -- cluster on VendorID
AS
SELECT * FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024;

-- Q6
SELECT DISTINCT(VendorID) FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_non_partitoned_2024
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15'; -- 310.24MB

SELECT DISTINCT(VendorID) FROM zoomcamp-dwh-450019.nytaxi.yellow_tripdata_partitioned_clustered_2024
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15'; -- 29.92MB