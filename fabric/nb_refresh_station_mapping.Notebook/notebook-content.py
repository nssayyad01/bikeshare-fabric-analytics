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
# META         }
# META       ]
# META     }
# META   }
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
