from pyspark.sql.functions import col, lit, current_date from delta.tables import DeltaTable

Define table paths

stage_table = "Tables/dbo.filetostage" target_table = "Tables/silver.cdc_qual"

Load staged data

df_stage = spark.read.format("delta").load(stage_table)

Add SCD2 columns

df_stage = df_stage.withColumn("effective_date", lit(current_date())) 
.withColumn("termination_date", lit("9999-12-31"))

Reorder columns: keys first, then audit columns, then the rest

selected_columns = [ "Client_ID", "Claim_Id",  # Keys "file_name", "effective_date", "termination_date",  # Audit columns "Survey_Designator", "Hosp_Service_Code", "Medical_Rec_No", "Race", "Carrier", "DOB", "Gender", "Last_Name", "First_Name", "Middle_Name", "Address1", "Address2", "City", "State", "Zip", "Discharge_Date", "Discharge_status_id", "Time_of_arrival", "Pt_type", "Nursing_station", "Sub_Group", "Minor_pt_type", "Attending_Physician", "Marital_Status", "Patient_Phone_Nbr", "Financial_Class", "Room", "Bed" ]

Select and reorder

df_stage = df_stage.select(*selected_columns)

Create the target table if it doesn't exist

if not DeltaTable.isDeltaTable(spark, target_table): df_stage.write.format("delta").save(target_table)

Load the target table as DeltaTable

delta_target = DeltaTable.forPath(spark, target_table)

Merge logic for Type 2 - key columns are Client_ID and Claim_Id

merge_condition = "tgt.Client_ID = stg.Client_ID AND tgt.Claim_Id = stg.Claim_Id AND tgt.termination_date = '9999-12-31'"

Columns to compare for changes (excluding keys and audit columns)

compare_columns = [ "Survey_Designator", "Hosp_Service_Code", "Medical_Rec_No", "Race", "Carrier", "DOB", "Gender", "Last_Name", "First_Name", "Middle_Name", "Address1", "Address2", "City", "State", "Zip", "Discharge_Date", "Discharge_status_id", "Time_of_arrival", "Pt_type", "Nursing_station", "Sub_Group", "Minor_pt_type", "Attending_Physician", "Marital_Status", "Patient_Phone_Nbr", "Financial_Class", "Room", "Bed" ]

update_condition = " OR ".join([f"tgt.{col} != stg.{col}" for col in compare_columns])

Perform the merge

delta_target.alias("tgt").merge( df_stage.alias("stg"), merge_condition ).whenMatchedUpdate( condition=update_condition, set={"termination_date": "current_date()"} ).whenNotMatchedInsert( values={col: "stg." + col for col in df_stage.columns} ).execute()

print("Type 2 CDC load to silver.cdc_qual completed.")
