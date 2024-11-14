# Databricks notebook source
# MAGIC %md
# MAGIC Don't forget to push this repo to
# MAGIC https://github.com/jstrassmayr/databricks_scd
# MAGIC
# MAGIC **Sources**
# MAGIC - https://docs.databricks.com/en/delta/merge.html#language-python
# MAGIC - https://iterationinsights.com/article/how-to-implement-slowly-changing-dimensions-scd-type-2-using-delta-table/
# MAGIC - https://www.youtube.com/watch?v=GhBlup-8JbE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # (Re)Create target dimension table 'dim_employee' and add initial data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.scd_example.dim_employee (
# MAGIC   surrogate_id LONG NOT NULL,
# MAGIC   id LONG NOT NULL,
# MAGIC   name STRING NOT NULL,
# MAGIC   age LONG,
# MAGIC   valid_from DATE NOT NULL,
# MAGIC   valid_to DATE,
# MAGIC   is_current BOOLEAN NOT NULL,
# MAGIC   hash LONG NOT NULL
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC INSERT INTO main.scd_example.dim_employee (surrogate_id, id, name, age, valid_from, valid_to, is_current, hash) 
# MAGIC VALUES (6, 22, 'Matt', 6, '2023-01-01', NULL, TRUE, xxhash64('Matt~6')),
# MAGIC (7, 33, 'John', 10, '2023-01-01', NULL, TRUE, xxhash64('John~10'));
# MAGIC
# MAGIC SELECT * FROM main.scd_example.dim_employee;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create some helper functions

# COMMAND ----------

from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Show the content of a delta table
def show_delta_table_content(delta_table : DeltaTable):
  dfTargetDimEmployee = delta_table.toDF()
  dfTargetDimEmployee = dfTargetDimEmployee.orderBy(F.asc("id"), F.asc("surrogate_id"))
  display(dfTargetDimEmployee)

# Add a hash column by using xxhash64 and adding '~' as separator between each column value to avoid collisions.
# The hash should be built using the actual dimensional attributes (aka. the "flesh") of the table.
def add_hash_column(df : DataFrame, columns_to_hash : list) -> DataFrame:
  df = df.withColumn("hash", F.lit(F.xxhash64(F.concat_ws("~", *columns_to_hash))))  
  return df

# Add a surrogate key per id and hash row (to make it unique in case of duplicates per (input) id).
# The surrogate key is called "tmp" as it does not reflect the "current maximum" value of the target table's surrogate key.
def add_tmp_surrogate_id_column(df : DataFrame, surrogate_base_keys = ['id', 'hash']) -> DataFrame:
  w = Window().orderBy(*surrogate_base_keys)  # ignore the warning regarding performance
  df = df.withColumn("tmp_surrogate_id", F.row_number().over(w).cast('long'))
  return df 

# COMMAND ----------

# MAGIC %md
# MAGIC # Read target

# COMMAND ----------

target_dim_employee_tablename = "main.scd_example.dim_employee"
target_dim_employee = DeltaTable.forName(spark, target_dim_employee_tablename)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read source (updates) data (mocked by DF)
# MAGIC
# MAGIC The source data is faked by creating a Dataframe with static values.

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True)
])

sourceUpdates0 = [
    (33, "John", 11),
    (44, "Mary", 20),
    (55, "Ben", 30)
]
df_updates_raw = spark.createDataFrame(sourceUpdates0, schema)
display(df_updates_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge source updates into target

# COMMAND ----------

# MAGIC %md
# MAGIC ## We need to add some columns to the source data
# MAGIC This will help us e.g. to
# MAGIC - find differences between source and target tables - by adding a hash
# MAGIC - have a unique (artificial) key to identify rows in the target - by adding a surrogate key
# MAGIC - ...

# COMMAND ----------

df_updates = df_updates_raw

df_updates = add_hash_column(df_updates, ['name', 'age'])
df_updates = add_tmp_surrogate_id_column(df_updates)
df_updates = df_updates.withColumn("is_current", F.lit(True))
display(df_updates)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ignore (source) rows if they are in target table already
# MAGIC So: Let's find the (source) rows that are different to the target and should trigger an insert/update.

# COMMAND ----------

df_target_dim_employee = target_dim_employee.toDF()

# A left-anti join returns only rows from the left table that are not matching any rows from the right table.
# If the hash is different per id, the row is different - compared to the target.
df_rows_to_update = (
    df_updates.alias("source")
    .where("is_current = true")
    .join(df_target_dim_employee.alias("target"), ["id", "hash", "is_current"], "leftanti")
    .orderBy(F.col("source.id"))
)

# Retrieve the current maximum surrogate key from the target table
maxTableKey = df_target_dim_employee.agg({"surrogate_id": "max"}).collect()[0][0] or 0    # or 0 if no rows in target table
print(f"Current max. surrogate key is {maxTableKey}")

# Calculate the new surrogate key
df_rows_to_update = df_rows_to_update.withColumn("surrogate_id", F.col("tmp_surrogate_id") + maxTableKey)
display(df_rows_to_update)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deactivate rows in the target
# MAGIC ... if new rows for the ID are coming from the source.

# COMMAND ----------

# Merge statement to expire old records
target_dim_employee.alias("target") \
  .merge(source = df_rows_to_update.alias("updates"), condition = 'target.id = updates.id') \
  .whenMatchedUpdate(
    condition = "target.is_current = True AND target.hash <> updates.hash",     # Invalidate the current row where any value (hash) changed
    set = {                                      
      "is_current": F.lit(False),
      "valid_to": F.lit(F.current_timestamp())
    }).execute()

display(df_rows_to_update)
show_delta_table_content(target_dim_employee)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Insert new and updated records outside the previous .merge() statement

# COMMAND ----------

df_rows_to_insert = df_rows_to_update\
       .withColumn("surrogate_id", F.col("surrogate_id").cast("Long"))\
       .withColumn("valid_from", F.current_date()) \
       .withColumn("valid_to", F.lit(None).cast("date"))

df_rows_to_insert = df_rows_to_insert.select("surrogate_id", "id", "name", "age", "valid_from", "valid_to", "is_current", "hash")

# Write the DataFrame to the Delta table
df_rows_to_insert.write.format("delta").mode("append").saveAsTable(target_dim_employee_tablename)
display(df_rows_to_insert)

# COMMAND ----------

# MAGIC %md
# MAGIC # Show updated target dimension

# COMMAND ----------

show_delta_table_content(target_dim_employee)