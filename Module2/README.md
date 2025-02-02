#Module 2 Homework

1. yellow_tripdata_2020-12.csv size = 
2. file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"\
    inputs: green, 2020, 04\
    rendered: green_tripdata_2020-04.csv
3. 
SELECT COUNT(1) FROM zoomcamp.yellow_tripdata
WHERE filename like '%2020%'
rows: 

4. 
SELECT COUNT(1) FROM zoomcamp.green_tripdata
WHERE filename like '%2020%'
rows:

5. 
SELECT COUNT(1) FROM zoomcamp.yellow_tripdata
WHERE filename like '%2021-03%'

6. timezone: America/New_York