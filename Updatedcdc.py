from pyspark.sql.functions import col, lit, current_date from delta.tables import DeltaTable

Load staged data directly from table written by pipeline

df_stage = spark.read.table("dbo.FileToStage")

Add SCD2 audit columns

df_stage = df_stage.withColumn("effective_date", lit(current_date())) 
.withColumn("termination_date", lit("9999-12-31"))

Define desired columns (keys + audit + business columns)

desired_columns = [ "Client_ID", "Claim_Id",  # Keys "effective_date", "termination_date",  # Audit columns "Survey_Designator", "Hosp_Service_Code", "Medical_Rec_No", "Race", "Carrier", "DOB", "Gender", "Last_Name", "First_Name", "Middle_Name", "Address1", "Address2", "City", "State", "Zip", "Discharge_Date", "Discharge_status_id", "Time_of_arrival", "Pt_type", "Nursing_station", "Sub_Group", "Minor_pt_type", "Attending_Physician", "Marital_Status", "Patient_Phone_Nbr", "Financial_Class", "Room", "Bed" ]

Filter only columns that exist in the DataFrame

available_columns = [col for col in desired_columns if col in df_stage.columns] missing_columns = [col for col in desired_columns if col not in df_stage.columns]

Optional: Print missing columns

if missing_columns: print("Missing columns (skipped):", missing_columns)

Select only valid columns for processing

df_stage = df_stage.select(*available_columns)

Define target path

target_table = "Tables/Silver/cdc_qual"

Create the target table if it doesn't exist

if not DeltaTable.isDeltaTable(spark, target_table): df_stage.write.format("delta").save(target_table)

Load target table

delta_target = DeltaTable.forPath(spark, target_table)

Merge logic for Type 2

merge_condition = "tgt.Client_ID = stg.Client_ID AND tgt.Claim_Id = stg.Claim_Id AND tgt.termination_date = '9999-12-31'"

Determine which columns to compare (excluding keys and audit columns)

comparison_columns = [col for col in available_columns if col not in ["Client_ID", "Claim_Id", "effective_date", "termination_date"]] update_condition = " OR ".join([f"tgt.{col} != stg.{col}" for col in comparison_columns])

Execute Type 2 merge

delta_target.alias("tgt").merge( df_stage.alias("stg"), merge_condition ).whenMatchedUpdate( condition=update_condition, set={"termination_date": "current_date()"} ).whenNotMatchedInsert( values={col: f"stg.{col}" for col in available_columns} ).execute()

print("CDC Type 2 load completed.")


