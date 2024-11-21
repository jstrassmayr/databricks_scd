# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Process
# MAGIC
# MAGIC To perform CDC processing with APPLY CHANGES, you need a streaming source and a streaming target table.
# MAGIC If your actual source is not a streaming table but a Delta table, you can set that table's properties to enable the Change Data Feed. This feed can be "filled" into a streaming view or table which is the source for the actual target dimension/table. 
# MAGIC To merge the source streaming table into the target, you 
# MAGIC 1. First create a streaming table and then 
# MAGIC 1. Use the APPLY CHANGES INTO statement to specify the source, keys, and sequencing for the change feed.
# MAGIC
# MAGIC ## Prerequisits
# MAGIC - The source- and target-table must be Streaming tables
# MAGIC - You must specify a column in the source data on which to sequence records. This column must be a sortable data type.
# MAGIC - To use the CDC APIs, your pipeline must be configured to use serverless DLT pipelines or the Delta Live Tables Pro or Advanced editions.
# MAGIC
# MAGIC ## Sources
# MAGIC - https://docs.databricks.com/en/delta-live-tables/cdc.html
# MAGIC - https://docs.databricks.com/en/delta-live-tables/python-ref.html#cdc

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a DLT view based on the CDF from the source table
# MAGIC In order to have a streaming table as source dataset (see pre-requisits), we create a DLT view
# MAGIC
# MAGIC Be aware that the DLT view is not visible after the execution of the DLT pipeline.
# MAGIC
# MAGIC For debug purposes this view could be converted into a DLT table (aka. materialized view) using the @dlt.table annotation.

# COMMAND ----------

# MAGIC %md
# MAGIC The defined view adds a new column `surrogate_id1` to the `dlt_employee` table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr

@dlt.view   
def dlt_employee_streaming():
  df = spark.readStream.format("delta").option("readChangeData", "true").table("main.scd_example.dlt_employee")
  df = df.withColumn("surrogate_id1", F.xxhash64('id', 'sys_edit_dt'))
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the target table with SCD2 and APPLY_CHANGES
# MAGIC
# MAGIC The creation of the target table is done without schema etc. as it is filled using APPLY_CHANGES().
# MAGIC
# MAGIC **Note**
# MAGIC - We don't write column sys_edit_dt to the target table as it is reflected in __START_AT because sys_edit_dt is defined to be the sequence_by column.
# MAGIC - We don't write columns _change_type, _commit_version and _commit_timestamp to the target as they might cause conflicts with the internal columns of the same name in the target streaming table.

# COMMAND ----------

dlt.create_streaming_table("dlt_dim_employee")

dlt.apply_changes(
  target = "dlt_dim_employee",
  source = "dlt_employee_streaming",
  keys = ["id"],
  sequence_by = col("sys_edit_dt"),
  except_column_list = ["sys_edit_dt", "_change_type", "_commit_version", "_commit_timestamp"],
  stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Surrogate Key (alternative) using an extra table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

@dlt.table
def dlt_dim_employee_with_surrogate_id():
    df_target_dim_employee = dlt.readStream("dlt_dim_employee")      # Load the data from the existing table
    df_target_dim_employee = df_target_dim_employee.withColumn("surrogate_id2", F.xxhash64('id', '__START_AT'))
    return df_target_dim_employee
