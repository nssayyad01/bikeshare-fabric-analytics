# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "29e56c90-a46f-44a6-8170-a825211cc928",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "cd316822-5b27-4bcb-b475-4ea19e712e4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "29e56c90-a46f-44a6-8170-a825211cc928"
# META         },
# META         {
# META           "id": "5ea67565-6c2a-407d-b17b-9e738198d308"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

trips = spark.table("lh_silver.dbo.trips")
st = spark.table("lh_bronze.public.stations")

print("Trip ID samples:")
trips.groupBy("source_schema").agg(
    F.first("start_station_id").alias("sample_id"),
    F.countDistinct("start_station_id").alias("distinct_start_ids"),
).show(truncate=False)

print("Stations table samples:")
st.select("station_id", "station_name").show(3, truncate=False)

names = (st.select(F.lower(F.trim("station_name")).alias("n"))
           .distinct().withColumn("in_stations", F.lit(True)))

tn = (trips.select("source_schema", F.lower(F.trim("start_station_name")).alias("n"))
           .where("n is not null").distinct())

tn.join(names, "n", "left").groupBy("source_schema").agg(
    F.count("*").alias("distinct_trip_station_names"),
    F.count("in_stations").alias("matched_in_stations_table"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
c = F.col

trips = spark.table("lh_silver.dbo.trips")
st = spark.table("lh_bronze.public.stations")

cur = (st.select("station_id", F.lower(F.trim("station_name")).alias("n"))
         .groupBy("n").agg(F.min("station_id").alias("station_id"),
                           F.count("*").alias("dup")))
print("Duplicate normalized names in stations:", cur.filter("dup > 1").count())

start = trips.select("source_schema", c("start_station_id").alias("trip_station_id"),
                     c("start_station_name").alias("trip_station_name"),
                     c("start_lat").alias("lat"), c("start_lng").alias("lng"))
end = trips.select("source_schema", c("end_station_id").alias("trip_station_id"),
                   c("end_station_name").alias("trip_station_name"),
                   c("end_lat").alias("lat"), c("end_lng").alias("lng"))

station_map = (
    start.unionByName(end)
    .where("trip_station_name is not null")
    .groupBy("source_schema", "trip_station_id", "trip_station_name")
    .agg(F.avg("lat").alias("lat"), F.avg("lng").alias("lng"),
         F.count("*").alias("trip_mentions"))
    .withColumn("n", F.lower(F.trim("trip_station_name")))
    .join(cur.select("n", "station_id"), "n", "left")
    .withColumn("match_status",
                F.when(c("station_id").isNotNull(), "matched").otherwise("historical_only"))
    .drop("n")
    .withColumn("_loaded_at", F.current_timestamp())
)

station_map.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.station_map")

spark.sql("""
    SELECT source_schema, match_status,
           count(*) AS station_rows,
           sum(trip_mentions) AS trip_mentions,
           round(100 * sum(trip_mentions) /
                 sum(sum(trip_mentions)) OVER (PARTITION BY source_schema), 1) AS pct_of_mentions
    FROM lh_silver.dbo.station_map
    GROUP BY source_schema, match_status
    ORDER BY 1, 2
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

st = spark.table("lh_bronze.public.stations").withColumn(
    "n", F.lower(F.trim("station_name")))

dups = st.groupBy("n").count().filter("count > 1").select("n")

st.join(dups, "n").select(
    "station_id", "station_name", "capacity", "lat", "lon", "status"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

spark.table("lh_silver.dbo.gbfs_station_status") \
    .where(F.col("station_id").isin(
        "66dbc420-0aca-11e7-82f6-3863bb44ef7c",
        "2098714441479661752")) \
    .groupBy("station_id").agg(
        F.count("*").alias("snapshots"),
        F.max("last_reported_ts").alias("last_reported"),
        F.max(F.col("num_bikes_available") + F.col("num_docks_available")).alias("max_bikes_plus_docks"),
        F.max("is_installed").alias("is_installed"),
        F.max("is_renting").alias("is_renting"),
    ).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

spark.table("lh_silver.dbo.station_map") \
    .where(F.lower(F.trim("trip_station_name")) == "clinton st & grand st") \
    .select("source_schema", "trip_station_id", "trip_station_name",
            "trip_mentions", "station_id", "match_status") \

    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

st = spark.table("lh_bronze.public.stations")
names = [r["n"] for r in (
    st.select(F.lower(F.trim("station_name")).alias("n"))
      .groupBy("n").count().filter("count > 1").select("n")
      .collect()
)]
print("Ambiguous names:", names)

dt = DeltaTable.forName(spark, "lh_silver.dbo.station_map")
dt.update(
    condition=F.lower(F.trim(F.col("trip_station_name"))).isin(names),
    set={"station_id": F.lit(None).cast("string"),
         "match_status": F.lit("ambiguous_name")},
)

spark.sql("""
    SELECT source_schema, match_status,
           count(*) AS station_rows,
           sum(trip_mentions) AS trip_mentions,
           round(100 * sum(trip_mentions) /
                 sum(sum(trip_mentions)) OVER (PARTITION BY source_schema), 1) AS pct_of_mentions
    FROM lh_silver.dbo.station_map
    GROUP BY source_schema, match_status
    ORDER BY 1, 2
""").show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
c = F.col

stn = spark.table("lh_bronze.public.stations")
tkt = spark.table("lh_bronze.public.maintenance_tickets")
tsk = spark.table("lh_bronze.public.rebalancing_tasks")

for name, df in [("stations", stn), ("tickets", tkt), ("tasks", tsk)]:
    print(name, "rows:", df.count())
    df.printSchema()

print("dup station_id:", stn.count() - stn.select("station_id").distinct().count())
print("dup ticket_id:", tkt.count() - tkt.select("ticket_id").distinct().count())
print("dup task_id:", tsk.count() - tsk.select("task_id").distinct().count())

print("stations capacity <= 0:", stn.filter("capacity <= 0").count())
stn.groupBy("status").count().show()
tkt.groupBy("issue_category", "status").count().orderBy("issue_category", "status").show()
tsk.groupBy("status").count().show()
print("tasks from == to:", tsk.filter("from_station_id = to_station_id").count())
print("completed without completed_at:",
      tsk.filter("status = 'completed' AND completed_at IS NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable
c = F.col

def upsert(df, table, key):
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        print(table, "created")
    else:
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute())
        print(table, "merged")

stn = (spark.table("lh_bronze.public.stations")
       .withColumn("capacity_valid", c("capacity") > 0)
       .withColumn("station_name_norm", F.lower(F.trim("station_name")))
       .withColumn("_loaded_at", F.current_timestamp()))

tkt = (spark.table("lh_bronze.public.maintenance_tickets")
       .withColumn("technician_note", F.trim("technician_note"))
       .withColumn("is_open", c("status") == "open")
       .withColumn("doc_id", F.concat(F.lit("ticket_"), c("ticket_id").cast("string")))
       .withColumn("_loaded_at", F.current_timestamp()))

tsk = (spark.table("lh_bronze.public.rebalancing_tasks")
       .withColumn("is_completed", c("status") == "completed")
       .withColumn("duration_minutes",
                   (F.unix_timestamp("completed_at") - F.unix_timestamp("assigned_at")) / 60)
       .withColumn("_loaded_at", F.current_timestamp()))

upsert(stn, "lh_silver.dbo.stations", "station_id")
upsert(tkt, "lh_silver.dbo.maintenance_tickets", "ticket_id")
upsert(tsk, "lh_silver.dbo.rebalancing_tasks", "task_id")

spark.sql("""
    SELECT 'stations' AS t, count(*) AS rows_, sum(CASE WHEN capacity_valid THEN 0 ELSE 1 END) AS flagged FROM lh_silver.dbo.stations
    UNION ALL
    SELECT 'tickets', count(*), sum(CASE WHEN is_open THEN 1 ELSE 0 END) FROM lh_silver.dbo.maintenance_tickets
    UNION ALL
    SELECT 'tasks', count(*), sum(CASE WHEN is_completed THEN 1 ELSE 0 END) FROM lh_silver.dbo.rebalancing_tasks
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Incremental Load 

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable
c = F.col

T = "lh_silver.dbo.gbfs_station_status"
BASE ="abfss://cd316822-5b27-4bcb-b475-4ea19e712e4a@onelake.dfs.fabric.microsoft.com/5ea67565-6c2a-407d-b17b-9e738198d308/Files/gbfs/station_status/"

all_files = [f.name for f in notebookutils.fs.ls(BASE)
             if f.name.startswith("station_status_") and f.name.endswith(".json")]
done = {r[0] for r in spark.table(T).select("source_file_name").distinct().collect()}
new_files = sorted(set(all_files) - done)
print("Total files:", len(all_files), "| already in Silver:", len(done), "| new:", len(new_files))

if new_files:
    raw = (spark.read.option("multiLine", True)
           .json([BASE + n for n in new_files])
           .withColumn("source_file", F.input_file_name()))
    flat = (raw.select(c("last_updated"), c("source_file"),
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
            F.regexp_extract("source_file", r"([^/]+)$", 1).alias("source_file_name"),
        )
        .dropDuplicates(["station_id", "snapshot_ts"])
        .withColumn("_loaded_at", F.current_timestamp()))

    (DeltaTable.forName(spark, T).alias("t")
        .merge(flat.alias("s"),
               "t.station_id = s.station_id AND t.snapshot_ts = s.snapshot_ts")
        .whenNotMatchedInsertAll()
        .execute())

print(spark.table(T).count(), "rows in", T)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT station_id, station_name, capacity, capacity_valid, updated_at
    FROM lh_silver.dbo.stations
    WHERE updated_at >= '2026-10-11'
    ORDER BY updated_at
    LIMIT 5
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
