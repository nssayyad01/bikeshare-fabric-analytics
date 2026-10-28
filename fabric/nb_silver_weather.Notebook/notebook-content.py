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

# CELL ********************

from pyspark.sql import functions as F

wraw = (
    spark.read.option("multiLine", True)
    .json("Files/weather/open_meteo/weather_*.json")
    .withColumn("source_file", F.input_file_name())
)

print("Files read:", wraw.select("source_file").distinct().count())
wraw.printSchema()
wraw.select(
    "latitude", "longitude", "timezone", "utc_offset_seconds",
    F.size("hourly.time").alias("n_hours"),
    F.col("hourly.time")[0].alias("first_time"),
).show(3, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
c = F.col

spark.conf.set("spark.sql.session.timeZone", "UTC")

hourly = (
    wraw
    .select("source_file", "hourly", F.posexplode("hourly.time").alias("pos", "time_local"))
    .select(
        F.to_utc_timestamp(
            F.to_timestamp("time_local", "yyyy-MM-dd'T'HH:mm"), "America/New_York"
        ).alias("hour_ts_utc"),
        c("hourly.temperature_2m").getItem(c("pos")).alias("temperature_c"),
        c("hourly.precipitation").getItem(c("pos")).alias("precipitation_mm"),
        c("hourly.wind_speed_10m").getItem(c("pos")).alias("wind_speed_kmh"),
        F.to_timestamp(
            F.regexp_extract("source_file", r"weather_(\d{8}_\d{6})\.json", 1),
            "yyyyMMdd_HHmmss",
        ).alias("fetched_at"),
        F.regexp_extract("source_file", r"([^/]+)$", 1).alias("source_file_name"),
    )
)

latest_first = Window.partitionBy("hour_ts_utc").orderBy(c("fetched_at").desc())

silver_weather = (
    hourly
    .withColumn("rn", F.row_number().over(latest_first))
    .filter("rn = 1").drop("rn")
    .withColumn("is_forecast", c("hour_ts_utc") > c("fetched_at"))
    .withColumn("_loaded_at", F.current_timestamp())
)

print("Rows before dedup:", hourly.count(), "| after dedup:", silver_weather.count())

silver_weather.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.weather_hourly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT count(*) AS rows_,
           count(DISTINCT hour_ts_utc) AS distinct_hours,
           min(hour_ts_utc) AS first_hour,
           max(hour_ts_utc) AS last_hour,
           sum(CASE WHEN is_forecast THEN 1 ELSE 0 END) AS forecast_rows,
           sum(CASE WHEN temperature_c IS NULL THEN 1 ELSE 0 END) AS null_temp
    FROM lh_silver.dbo.weather_hourly
""").show(truncate=False)

print(wraw.select("hourly_units.*").first())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
c = F.col
spark.conf.set("spark.sql.session.timeZone", "UTC")

hraw = (
    spark.read.option("multiLine", True)
    .json("Files/weather/open_meteo_history/[Ww]eather_hist_*.json")
    .withColumn("source_file", F.input_file_name())
)

hist = (
    hraw
    .select("source_file", "hourly", F.posexplode("hourly.time").alias("pos", "time_local"))
    .select(
        F.to_utc_timestamp(
            F.to_timestamp("time_local", "yyyy-MM-dd'T'HH:mm"), "America/New_York"
        ).alias("hour_ts_utc"),
        c("hourly.temperature_2m").getItem(c("pos")).alias("temperature_c"),
        c("hourly.precipitation").getItem(c("pos")).alias("precipitation_mm"),
        c("hourly.wind_speed_10m").getItem(c("pos")).alias("wind_speed_kmh"),
        F.regexp_extract("source_file", r"([^/]+)$", 1).alias("source_file_name"),
    )
    .withColumn("is_forecast", F.lit(False))
    .withColumn("_loaded_at", F.current_timestamp())
)

hist.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.weather_hourly_history")

spark.sql("""
    SELECT source_file_name,
           count(*) AS hours,
           count(DISTINCT hour_ts_utc) AS distinct_hours,
           min(hour_ts_utc) AS first_hour,
           max(hour_ts_utc) AS last_hour,
           sum(CASE WHEN temperature_c IS NULL THEN 1 ELSE 0 END) AS null_temp,
           sum(CASE WHEN precipitation_mm IS NULL THEN 1 ELSE 0 END) AS null_precip
    FROM lh_silver.dbo.weather_hourly_history
    GROUP BY source_file_name
    ORDER BY source_file_name
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT max(hour_ts_utc) AS last_actual_hour
    FROM lh_silver.dbo.weather_hourly
    WHERE is_forecast = false
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
