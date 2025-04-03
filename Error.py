metrics_df = spark.createDataFrame(metrics_row, schema=metrics_schema)

# Force all numeric columns to match existing schema (e.g., LongType)
metrics_df = metrics_df.select(
    col("job_start_time"),
    col("job_end_time"),
    col("source_count").cast("long"),
    col("insert_count").cast("long"),
    col("update_count").cast("long"),
    col("delete_count").cast("long"),
    col("no_change_count").cast("long"),
    col("source_file")
)
if not DeltaTable.isDeltaTable(spark, "dbo.cdc_control"):
    metrics_df.write.format("delta").saveAsTable("dbo.cdc_control")
else:
    metrics_df.write.format("delta").mode("append").saveAsTable("dbo.cdc_control")
