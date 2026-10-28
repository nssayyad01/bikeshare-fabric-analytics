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
c = F.col
T = "lh_silver.dbo.gbfs_station_status"


loaded = [r[0] for r in spark.table(T).select("source_file_name").distinct().collect()]


raw_all = (
    spark.read.option("multiLine", True)
    .json("Files/gbfs/station_status/station_status_*.json")
    .withColumn("source_file_name", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
)
new_raw = raw_all.filter(~c("source_file_name").isin(loaded))
n_new = new_raw.select("source_file_name").distinct().count()
print("Already loaded files:", len(loaded), "| new files:", n_new)

if n_new > 0:
    flat = (
        new_raw
        .select(c("last_updated"), c("source_file_name"),
                F.explode("data.stations").alias("s"))
        .select(
            c("s.station_id").alias("station_id"),
            F.to_timestamp(F.from_unixtime("last_updated")).alias("snapshot_ts"),
            F.to_timestamp(F.from_unixtime(c("s.last_reported"))).alias("last_reported_ts"),
            c("s.num_bikes_available").cast("int").alias("num_bikes_available"),
            c("s.num_ebikes_available").cast("int").alias("num_ebikes_available"),
            c("s.num_bikes_disabled").cast("int").alias("num_bikes_disabled"),
            c("s.num_docks_available").cast("int").alias("num_docks_available"),
            c("s.num_docks_disabled").cast("int").alias("num_docks_disabled"),
            c("s.is_installed").cast("int").alias("is_installed"),
            c("s.is_renting").cast("int").alias("is_renting"),
            c("s.is_returning").cast("int").alias("is_returning"),
            c("source_file_name"),
        )
        .dropDuplicates(["station_id", "snapshot_ts"])
        .withColumn("_loaded_at", F.current_timestamp())
    )
    flat.write.format("delta").mode("append").saveAsTable(T)
    print("Appended rows:", flat.count())


spark.sql(f"""
    SELECT count(*) AS total_rows,
           count(DISTINCT snapshot_ts) AS snapshots,
           count(DISTINCT source_file_name) AS files,
           max(snapshot_ts) AS last_snapshot
    FROM {T}
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
