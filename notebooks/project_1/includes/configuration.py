# Databricks notebook source
storage_account_name = "formula21adls"
storage_account_key  = "fya2EQ6wohfeoQOGAziyKKTcxTKlvKAfnS7eWobR/kIMPFSdY9ZKedOxGuPnoG7K2xmzvPNVpo5y+AStA8swzg=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

container_name = "raw" 
container_name1 = "processed"
container_name2  = "presentation"

# COMMAND ----------

raw_path = "abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
processed_path = "abfss://{container_name1}@{storage_account_name}.dfs.core.windows.net"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

