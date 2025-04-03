
if not DeltaTable.isDeltaTable(spark, "dbo.cdc_control"):
    metrics_df.write.format("delta").saveAsTable("dbo.cdc_control")
else:
    metrics_df.write.format("delta").mode("append").saveAsTable("dbo.cdc_control")
