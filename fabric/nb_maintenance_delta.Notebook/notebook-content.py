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

tables = [
    "lh_silver.dbo.gbfs_station_status",
    "lh_silver.dbo.weather_hourly",
    "lh_silver.dbo.trips",
]

for t in tables:
    before = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles").first()[0]
    spark.sql(f"OPTIMIZE {t}")
    spark.sql(f"VACUUM {t} RETAIN 168 HOURS")
    after = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles").first()[0]
    print(f"{t}: files {before} -> {after}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
