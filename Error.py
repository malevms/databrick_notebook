# Step 3: Handle deletions — expire records not in the source
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
