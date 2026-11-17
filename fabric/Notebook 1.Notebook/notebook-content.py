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

ids = ['2285511427826713356','4281e975-6bc2-483f-9e4a-f7c09b67ed28',
       '479befd9-61f0-4524-a61c-fcb9283fce7e','2278012504182672958',
       '2286082034448823844']

df = spark.table("lh_bronze.public.stations")
print(df.columns)                      # columns ke naam dekhne ke liye
df.filter(df.station_id.isin(ids)).show(truncate=False)
print("Total stations in Bronze:", df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
