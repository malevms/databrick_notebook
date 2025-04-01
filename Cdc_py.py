
from pyspark.sql.functions import col, lit, current_date from delta.tables import DeltaTable

Define table paths

stage_table = "Tables/dbo.filetostage" target_table = "Tables/silver.target_table"

Load staged data

df_stage = spark.read.format("delta").load(stage_table)

Add SCD2 columns

df_stage = df_stage.withColumn("effective_date", lit(current_date())) 
.withColumn("termination_date", lit("9999-12-31"))

Select only the first 30 relevant columns for CDC (plus tracking columns)

df_stage = df_stage.select( "Survey_Designator", "Client_ID", "Hosp_Service_Code", "Medical_Rec_No", "Claim_Id", "Race", "Carrier", "DOB", "Gender", "Last_Name", "First_Name", "Middle_Name", "Address1", "Address2", "City", "State", "Zip", "Discharge_Date", "Discharge_status_id", "Time_of_arrival", "Pt_type", "Nursing_station", "Sub_Group", "Minor_pt_type", "Attending_Physician", "Marital_Status", "Patient_Phone_Nbr", "Financial_Class", "Room", "Bed", "file_name", "effective_date", "termination_date" )

Create the target table if it doesn't exist

if not DeltaTable.isDeltaTable(spark, target_table): df_stage.write.format("delta").save(target_table)

Load the target table as DeltaTable

delta_target = DeltaTable.forPath(spark, target_table)

Merge logic for Type 2 - key column is Client_ID

merge_condition = "tgt.Client_ID = stg.Client_ID AND tgt.termination_date = '9999-12-31'" update_condition = " OR ".join([ "tgt.{0} != stg.{0}".format(col) for col in [ "Survey_Designator", "Hosp_Service_Code", "Medical_Rec_No", "Claim_Id", "Race", "Carrier", "DOB", "Gender", "Last_Name", "First_Name", "Middle_Name", "Address1", "Address2", "City", "State", "Zip", "Discharge_Date", "Discharge_status_id", "Time_of_arrival", "Pt_type", "Nursing_station", "Sub_Group", "Minor_pt_type", "Attending_Physician", "Marital_Status", "Patient_Phone_Nbr", "Financial_Class", "Room", "Bed" ] ])

Perform the merge

delta_target.alias("tgt").merge( df_stage.alias("stg"), merge_condition ).whenMatchedUpdate( condition=update_condition, set={"termination_date": "current_date()"} ).whenNotMatchedInsert( values={col: "stg." + col for col in df_stage.columns} ).execute()

print("Type 2 CDC load to silver.target_table completed.")

