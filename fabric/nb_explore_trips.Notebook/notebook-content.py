# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5ea67565-6c2a-407d-b17b-9e738198d308",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "cd316822-5b27-4bcb-b475-4ea19e712e4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "5ea67565-6c2a-407d-b17b-9e738198d308"
# META         },
# META         {
# META           "id": "29e56c90-a46f-44a6-8170-a825211cc928"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Reading CSV

# CELL ********************

df13 = spark.read.option("header", True).csv("Files/trips/2013/201312-citibike-tripdata.csv")
df24 = spark.read.option("header", True).csv("Files/trips/2024/202401-citibike-tripdata_2.csv")

print("2013 rows:", df13.count())
print("2013 columns:", df13.columns)
print("2024 rows:", df24.count())
print("2024 columns:", df24.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Creating Common Schema 

# CELL ********************

from pyspark.sql import functions as F
c = F.col

t13 = df13.select(
    F.sha2(F.concat_ws("|", c("bikeid"), c("starttime"), c("start station id")), 256).alias("trip_id"),
    c("starttime").cast("timestamp").alias("started_at"),
    c("stoptime").cast("timestamp").alias("ended_at"),
    c("tripduration").cast("int").alias("duration_sec"),
    c("start station id").alias("start_station_id"),
    c("start station name").alias("start_station_name"),
    c("start station latitude").cast("double").alias("start_lat"),
    c("start station longitude").cast("double").alias("start_lng"),
    c("end station id").alias("end_station_id"),
    c("end station name").alias("end_station_name"),
    c("end station latitude").cast("double").alias("end_lat"),
    c("end station longitude").cast("double").alias("end_lng"),
    F.when(c("usertype") == "Subscriber", "member")
     .when(c("usertype") == "Customer", "casual").alias("user_type"),
    F.lit("unknown").alias("rideable_type"),
    F.lit("v2013").alias("source_schema"),
)

t24 = df24.select(
    c("ride_id").alias("trip_id"),
    c("started_at").cast("timestamp").alias("started_at"),
    c("ended_at").cast("timestamp").alias("ended_at"),
    (F.unix_timestamp(c("ended_at").cast("timestamp"))
     - F.unix_timestamp(c("started_at").cast("timestamp"))).cast("int").alias("duration_sec"),
    c("start_station_id"), c("start_station_name"),
    c("start_lat").cast("double"), c("start_lng").cast("double"),
    c("end_station_id"), c("end_station_name"),
    c("end_lat").cast("double"), c("end_lng").cast("double"),
    c("member_casual").alias("user_type"),
    c("rideable_type"),
    F.lit("v2024").alias("source_schema"),
)

trips = t13.unionByName(t24)

trips.groupBy("source_schema").count().show()
trips.groupBy("source_schema").agg(
    F.count(F.when(c("started_at").isNull(), 1)).alias("null_started_at")
).show()
trips.printSchema()
trips.show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Data Quality Checks


# CELL ********************

print("Duplicate trip_id:", trips.count() - trips.select("trip_id").distinct().count())

trips.groupBy("source_schema", "user_type").count().show()

trips.agg(
    F.count(F.when(c("duration_sec") <= 0, 1)).alias("duration_zero_or_negative"),
    F.count(F.when(c("ended_at") < c("started_at"), 1)).alias("end_before_start"),
    F.count(F.when(c("start_station_id").isNull(), 1)).alias("null_start_station"),
    F.count(F.when(c("end_station_id").isNull(), 1)).alias("null_end_station"),
    F.min("started_at").alias("min_started"),
    F.max("started_at").alias("max_started"),
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Null Investigation

# CELL ********************

trips.groupBy("source_schema").agg(
    F.count(F.when(c("start_station_id").isNull(), 1)).alias("null_start"),
    F.count(F.when(c("end_station_id").isNull(), 1)).alias("null_end"),
    F.count(F.when(c("start_lat").isNull(), 1)).alias("null_start_lat"),
    F.count(F.when(c("end_lat").isNull(), 1)).alias("null_end_lat"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips = (
    trips
    .withColumn("has_missing_station",
                c("start_station_id").isNull() | c("end_station_id").isNull())
    .withColumn("_loaded_at", F.current_timestamp())
)

silver_trips.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.trips")

spark.sql("""
    SELECT source_schema, has_missing_station, count(*) AS n
    FROM lh_silver.dbo.trips
    GROUP BY source_schema, has_missing_station
    ORDER BY 1, 2
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
