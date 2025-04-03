from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define the schema
schema = StructType([
    StructField("job_start_time", TimestampType(), True),
    StructField("job_end_time", TimestampType(), True),
    StructField("source_count", IntegerType(), True),
    StructField("insert_count", IntegerType(), True),
    StructField("update_count", IntegerType(), True),
    StructField("delete_count", IntegerType(), True),
    StructField("no_change_count", IntegerType(), True),
    StructField("source_file", StringType(), True)
])

# Create an empty DataFrame
empty_df = spark.createDataFrame([], schema)

# Write the control table (overwrite if re-creating)
empty_df.write.format("delta").mode("overwrite").saveAsTable("cdc_control")

print("Control table 'cdc_control' created successfully.")
