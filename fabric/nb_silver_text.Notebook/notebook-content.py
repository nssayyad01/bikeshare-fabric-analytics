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
c = F.col

t = spark.table("lh_silver.dbo.maintenance_tickets")
note = F.lower(c("technician_note"))

pred = (F.when(note.rlike(r"\b(batter|charg)"), "battery")
         .when(note.rlike(r"\bbrak"), "brake")
         .when(note.rlike(r"\b(tire|tyre|puncture|inflat)"), "tire")
         .when(note.rlike(r"\b(screen|kiosk|touch|payment)"), "screen")
         .when(note.rlike(r"\b(dock|lock)"), "dock")
         .otherwise("other"))

scored = (t.withColumn("predicted_category", pred)
           .withColumn("is_match", c("predicted_category") == c("issue_category")))

total = scored.count()
ok = scored.filter("is_match").count()
print(f"Accuracy: {ok}/{total} = {ok/total:.1%}")

scored.groupBy("issue_category", "predicted_category").count() \
      .orderBy("issue_category", "predicted_category").show(50)

scored.filter("NOT is_match") \
      .select("ticket_id", "issue_category", "predicted_category", "technician_note") \
      .show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

T = "lh_silver.dbo.text_docs"

docs = (scored.select(
            c("doc_id"),
            F.lit("maintenance_ticket").alias("source"),
            c("station_id"),
            c("created_at"),
            c("updated_at"),
            c("technician_note").alias("text"),
            c("issue_category").alias("label_original"),
            c("predicted_category"),
            c("is_match"),
            F.lit(None).cast("string").alias("predicted_sentiment"),
            F.current_timestamp().alias("_loaded_at")))

if not spark.catalog.tableExists(T):
    docs.write.format("delta").saveAsTable(T)
    print(T, "created")
else:
    (DeltaTable.forName(spark, T).alias("t")
        .merge(docs.alias("s"), "t.doc_id = s.doc_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())
    print(T, "merged")

spark.sql(f"""
    SELECT source, count(*) AS docs, count(DISTINCT doc_id) AS distinct_docs,
           sum(CASE WHEN is_match THEN 1 ELSE 0 END) AS matched
    FROM {T} GROUP BY source
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
c = F.col

fb = spark.table("lh_bronze.public.rider_feedback")
txt = F.lower(F.trim(c("feedback_text")))

cat = (F.when(txt.rlike(r"\b(charged|payment|unlock|login|logging|crash)|\bapp\b"), "app_payment")
        .when(txt.rlike(r"\b(prices?|fees?|fares?|billing|membership|paid)\b"), "pricing")
        .when(txt.rlike(r"\b(seat|chain|handlebar|wheel|gear|bell)"), "bike_condition")
        .when(txt.rlike(r"\b(great|love|thank you|smooth ride|plenty)"), "praise")
        .when(txt.rlike(r"\b(dock|slots|return|put the bike back)"), "dock_availability")
        .when(txt.rlike(r"\b(no bikes|zero bikes|not a single bike|nothing to ride|empty)"), "bike_availability")
        .otherwise("general"))

sent = (F.when(txt.rlike(r"\b(great|love|thank you|smooth|plenty|fixed)"), "positive")
         .when(txt.rlike(r"\b(okay|average|first time|rode from)"), "neutral")
         .otherwise("negative"))

scored = (fb.withColumn("pred_category", cat)
            .withColumn("pred_sentiment", sent)
            .withColumn("cat_ok", c("pred_category") == c("true_category"))
            .withColumn("sent_ok", c("pred_sentiment") == c("true_sentiment")))

n = scored.count()
print("Category accuracy:", scored.filter("cat_ok").count(), "/", n)
print("Sentiment accuracy:", scored.filter("sent_ok").count(), "/", n)

scored.groupBy("true_category", "pred_category").count() \
      .orderBy("true_category", "pred_category").show(50)

scored.filter("NOT cat_ok OR NOT sent_ok") \
      .select("feedback_id", "true_category", "pred_category",
              "true_sentiment", "pred_sentiment", "feedback_text") \
      .show(40, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

T = "lh_silver.dbo.text_docs"

if "predicted_sentiment" not in spark.table(T).columns:
    spark.sql(f"ALTER TABLE {T} ADD COLUMNS (predicted_sentiment STRING)")
    print("column added")

fb_docs = scored.select(
    F.concat(F.lit("feedback_"), c("feedback_id").cast("string")).alias("doc_id"),
    F.lit("rider_feedback").alias("source"),
    c("station_id"),
    c("created_at"),
    c("updated_at"),
    c("feedback_text").alias("text"),
    c("true_category").alias("label_original"),
    c("pred_category").alias("predicted_category"),
    c("cat_ok").alias("is_match"),
    c("pred_sentiment").alias("predicted_sentiment"),
    F.current_timestamp().alias("_loaded_at"),
)

(DeltaTable.forName(spark, T).alias("t")
    .merge(fb_docs.alias("s"), "t.doc_id = s.doc_id")
    .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
    .whenNotMatchedInsertAll()
    .execute())

spark.sql(f"""
    SELECT source, count(*) AS docs, count(DISTINCT doc_id) AS distinct_docs,
           sum(CASE WHEN is_match THEN 1 ELSE 0 END) AS matched,
           count(predicted_sentiment) AS with_sentiment
    FROM {T} GROUP BY source ORDER BY source
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
