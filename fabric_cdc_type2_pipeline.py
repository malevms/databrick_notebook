
from pyspark.sql.functions import col, lit, current_date, current_timestamp, when
from delta.tables import DeltaTable
from datetime import datetime

# Track start time
job_start_time = datetime.now()

# Load staged data
df_stage = spark.read.table("dbo.FileToStage")

# Add audit columns
df_stage = df_stage.withColumn("effective_date", lit(current_date())) \
                   .withColumn("termination_date", lit("9999-12-31"))

# Desired columns
desired_columns = [
    "Client_ID", "Claim_Id", "effective_date", "termination_date",
    "Survey_Designator", "Hosp_Service_Code", "Medical_Rec_No", "Race", "Carrier", "DOB", "Gender",
    "Last_Name", "First_Name", "Middle_Name", "Address1", "Address2", "City", "State", "Zip",
    "Discharge_Date", "Discharge_status_id", "Time_of_arrival", "Pt_type", "Nursing_station",
    "Sub_Group", "Minor_pt_type", "Attending_Physician", "Marital_Status", "Patient_Phone_Nbr",
    "Financial_Class", "Room", "Bed"
]
available_columns = [col for col in desired_columns if col in df_stage.columns]
df_stage = df_stage.select(*available_columns)

# Target path
target_table = "Tables/Silver/cdc_qual"
if not DeltaTable.isDeltaTable(spark, target_table):
    df_stage.write.format("delta").save(target_table)
delta_target = DeltaTable.forPath(spark, target_table)

# Merge logic
merge_condition = "tgt.Client_ID = stg.Client_ID AND tgt.Claim_Id = stg.Claim_Id AND tgt.termination_date = '9999-12-31'"
comparison_columns = [col for col in available_columns if col not in ["Client_ID", "Claim_Id", "effective_date", "termination_date"]]
update_condition = " OR ".join([f"tgt.{col} != stg.{col}" for col in comparison_columns])

# Expire changed records
delta_target.alias("tgt").merge(
    df_stage.alias("stg"),
    merge_condition
).whenMatchedUpdate(
    condition=update_condition,
    set={"termination_date": "current_date()"}
).execute()

# Insert new/changed records
delta_target.alias("tgt").merge(
    df_stage.alias("stg"),
    merge_condition
).whenNotMatchedInsert(
    values={col: f"stg.{col}" for col in available_columns}
).execute()

# Handle deletes
df_target_active = spark.read.format("delta").load(target_table).filter("termination_date = '9999-12-31'")
df_deletes = df_target_active.join(df_stage.select("Client_ID", "Claim_Id"), on=["Client_ID", "Claim_Id"], how="left_anti")
delete_count = df_deletes.count()
if delete_count > 0:
    delete_keys = df_deletes.select("Client_ID", "Claim_Id").distinct().collect()
    for row in delete_keys:
        delta_target.update(
            condition=f"Client_ID = '{row['Client_ID']}' AND Claim_Id = '{row['Claim_Id']}' AND termination_date = '9999-12-31'",
            set={"termination_date": "current_date()"}
        )

# Change summary
source_count = df_stage.count()
df_target_post = spark.read.format("delta").load(target_table).filter("termination_date = '9999-12-31'")
df_joined = df_stage.alias("stg").join(df_target_post.alias("tgt"), on=["Client_ID", "Claim_Id"], how="left")
df_changes = df_joined.withColumn("change_type", when(col("tgt.Client_ID").isNull(), "Insert")
                                  .when((col("stg.Survey_Designator") != col("tgt.Survey_Designator")) |
                                        (col("stg.Hosp_Service_Code") != col("tgt.Hosp_Service_Code")) |
                                        (col("stg.Medical_Rec_No") != col("tgt.Medical_Rec_No")), "Update")
                                  .otherwise("NoChange"))
change_summary = df_changes.groupBy("change_type").count().collect()
metrics = {row["change_type"]: row["count"] for row in change_summary}

insert_count = metrics.get("Insert", 0)
update_count = metrics.get("Update", 0)
no_change_count = metrics.get("NoChange", 0)
source_file = "your_file.csv"
job_end_time = datetime.now()

# Final metrics write
metrics_row = [(job_start_time, job_end_time, source_count, insert_count, update_count, delete_count, no_change_count, source_file)]
metrics_schema = ["job_start_time", "job_end_time", "source_count", "insert_count", "update_count", "delete_count", "no_change_count", "source_file"]
metrics_df = spark.createDataFrame(metrics_row, schema=metrics_schema)

# Ensure column types match exactly
metrics_df = metrics_df.select(
    col("job_start_time"),
    col("job_end_time"),
    col("source_count").cast("int"),
    col("insert_count").cast("int"),
    col("update_count").cast("int"),
    col("delete_count").cast("int"),
    col("no_change_count").cast("int"),
    col("source_file")
)

# Append to control table
metrics_df.write.format("delta").mode("append").saveAsTable("dbo.cdc_control")

print("CDC Type 2 load completed with logging.")
