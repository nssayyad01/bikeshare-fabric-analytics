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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
c = F.col
T = "lh_silver.dbo.weather_hourly"
L = "lh_silver.dbo.processed_files"

spark.conf.set("spark.sql.session.timeZone", "UTC")


loaded = [r[0] for r in spark.table(L)
          .filter("source = 'weather'").select("file_name").collect()]

wraw_all = (
    spark.read.option("multiLine", True)
    .json("Files/weather/open_meteo/weather_*.json")
    .withColumn("source_file_name", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
)
new_raw = wraw_all.filter(~c("source_file_name").isin(loaded))
n_new = new_raw.select("source_file_name").distinct().count()
print("Already loaded files:", len(loaded), "| new files:", n_new)

if n_new > 0:

    hourly = (
        new_raw
        .select("source_file_name", "hourly", F.posexplode("hourly.time").alias("pos", "time_local"))
        .select(
            F.to_utc_timestamp(
                F.to_timestamp("time_local", "yyyy-MM-dd'T'HH:mm"), "America/New_York"
            ).alias("hour_ts_utc"),
            c("hourly.temperature_2m")[c("pos")].alias("temperature_c"),
            c("hourly.precipitation")[c("pos")].alias("precipitation_mm"),
            c("hourly.wind_speed_10m")[c("pos")].alias("wind_speed_kmh"),
            F.to_timestamp(
                F.regexp_extract("source_file_name", r"weather_(\d{8}_\d{6})\.json", 1),
                "yyyyMMdd_HHmmss",
            ).alias("fetched_at"),
            c("source_file_name"),
        )
    )

   
    latest_first = Window.partitionBy("hour_ts_utc").orderBy(c("fetched_at").desc())
    new_hours = (
        hourly
        .withColumn("rn", F.row_number().over(latest_first))
        .filter("rn = 1").drop("rn")
        .withColumn("is_forecast", c("hour_ts_utc") > c("fetched_at"))
        .withColumn("_loaded_at", F.current_timestamp())
    )

    
    (DeltaTable.forName(spark, T).alias("t")
        .merge(new_hours.alias("s"), "t.hour_ts_utc = s.hour_ts_utc")
        .whenMatchedUpdateAll(condition="t.is_forecast = true AND s.is_forecast = false")
        .whenNotMatchedInsertAll()
        .execute())
    print("Merged hours:", new_hours.count())

    
    (new_raw.select(F.col("source_file_name").alias("file_name")).distinct()
        .withColumn("source", F.lit("weather"))
        .withColumn("processed_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(L))
    print("Logged files:", n_new)

# 7. Check
spark.sql(f"""
    SELECT count(*) AS rows_,
           count(DISTINCT hour_ts_utc) AS distinct_hours,
           max(hour_ts_utc) AS last_hour,
           sum(CASE WHEN is_forecast = false THEN 1 ELSE 0 END) AS actual_hours
    FROM {T}
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
