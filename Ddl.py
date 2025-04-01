from pyspark.sql.types import StructType, StructField, StringType, DateType

# Define the schema for the target table
schema = StructType([
    StructField("Survey_Designator", StringType(), True),
    StructField("Client_ID", StringType(), True),
    StructField("Hosp_Service_Code", StringType(), True),
    StructField("Medical_Rec_No", StringType(), True),
    StructField("Claim_Id", StringType(), True),
    StructField("Race", StringType(), True),
    StructField("Carrier", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Middle_Name", StringType(), True),
    StructField("Address1", StringType(), True),
    StructField("Address2", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("Discharge_Date", StringType(), True),
    StructField("Discharge_status_id", StringType(), True),
    StructField("Time_of_arrival", StringType(), True),
    StructField("Pt_type", StringType(), True),
    StructField("Nursing_station", StringType(), True),
    StructField("Sub_Group", StringType(), True),
    StructField("Minor_pt_type", StringType(), True),
    StructField("Attending_Physician", StringType(), True),
    StructField("Marital_Status", StringType(), True),
    StructField("Patient_Phone_Nbr", StringType(), True),
    StructField("Financial_Class", StringType(), True),
    StructField("Room", StringType(), True),
    StructField("Bed", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("effective_date", DateType(), True),
    StructField("termination_date", StringType(), True)
])

# Create an empty DataFrame with the schema
empty_df = spark.createDataFrame([], schema)

# Save it to silver schema as a Delta table
empty_df.write.format("delta").mode("overwrite").saveAsTable("silver.target_table")

print("Table silver.target_table created.")
