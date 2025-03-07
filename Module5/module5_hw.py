#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F


credentials_location = '/home/realadmin/.google/creds/zcamp-spark-28a6151fa616.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/realadmin/spark/spark-3.5.5-bin-hadoop3/jars/gcs-connector-hadoop3-latest.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext.getOrCreate(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# Question 1
spark.version # '3.5.5'

# Question 2
df = spark.read.parquet('gs://zcamp-nyc-taxi/pq/yellow/2024/*')
df = df.repartition(4)
df.write.parquet('gs://zcamp-nyc-taxi/yellow/2024/10/', mode='overwrite')

# Question 3
df.createOrReplaceTempView('trips')
df.filter(F.to_date(df.tpep_pickup_datetime) == "2024-10-15").count() # 128893

df_15th = spark.sql("""
    SELECT COUNT(1) FROM trips
    WHERE CAST(tpep_pickup_datetime AS Date) = '2024-10-15'
""").show() 

# Question 4
df_long = spark.sql("""
    SELECT DATEDIFF(hour,tpep_pickup_datetime, tpep_dropoff_datetime) as length
    FROM trips
    ORDER BY length DESC
    LIMIT 1;
""").show() # 162

# Question 6
df_zones = spark.read.csv('taxi_zone_lookup.csv', header=True)
df_zones.createOrReplaceTempView('zones')

df_trips_zones = df.join(df_zones, df.PULocationID == df_zones.LocationID).drop('LocationID')

df_trips_zones.columns

df_trips_zones.createOrReplaceTempView('trips_zones')

df_least = spark.sql("""
    SELECT PULocationID
    ,Zone
    ,COUNT(1) AS total_pickups
    FROM trips_zones
    GROUP BY PULocationID, Zone
    ORDER BY total_pickups ASC
    LIMIT 1;
""").show() # Governor's Island/



