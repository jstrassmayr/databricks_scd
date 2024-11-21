# Databricks notebook source
# MAGIC %md
# MAGIC # Process
# MAGIC This notebook demonstrates how to write changed data from a source table to a target table which supports [Slowly Changing Dimensions](https://www.wikiwand.com/de/articles/Slowly_Changing_Dimensions) Type 2.
# MAGIC
# MAGIC 1. Create target table
# MAGIC 1. Create source (mock) data
# MAGIC 1. Merge
# MAGIC     1. Add columns to source data
# MAGIC     1. Exire old rows in target
# MAGIC     1. Insert new rows in target
# MAGIC
# MAGIC **Drawback:** The Merge statement is not able to handle a changed row (=a row that is already in the target table) in one statement. In order do this (UPDATE the existing row to expire it and also inserting a new row containing the changed values) you have to implement the Merge statement followed by an Insert statement (as shown below).
# MAGIC
# MAGIC ## Sources
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
# MAGIC   surrogate_id LONG GENERATED ALWAYS AS IDENTITY,
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
# MAGIC INSERT INTO main.scd_example.dim_employee (id, name, age, valid_from, valid_to, is_current, hash) 
# MAGIC VALUES (22, 'Matt', 6, '2023-01-01', NULL, TRUE, xxhash64('Matt~6')),
# MAGIC (33, 'John', 10, '2023-01-01', NULL, TRUE, xxhash64('John~10'));
# MAGIC
# MAGIC SELECT * FROM main.scd_example.dim_employee;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create some helper functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

# Add a hash column by using xxhash64 and adding '~' as separator between each column value to avoid collisions.
# The hash should be built using the actual dimensional attributes (aka. the "flesh") of the table.
def add_hash_column(df : DataFrame, columns_to_hash : list) -> DataFrame:
  df = df.withColumn("hash", F.lit(F.xxhash64(F.concat_ws("~", *columns_to_hash))))  
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Get reference to target table

# COMMAND ----------

target_dim_employee_tablename = "main.scd_example.dim_employee"
df_target_dim_employee = spark.read.table(target_dim_employee_tablename)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read source (updates) data (mocked by DF)
# MAGIC
# MAGIC The source data is faked by creating a Dataframe with static values.

# COMMAND ----------

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
df_updates = df_updates.withColumn("is_current", F.lit(True))
display(df_updates)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ignore (source) rows if they are in target table already
# MAGIC So: Let's find the (source) rows that are different to the target and should trigger an insert/update.

# COMMAND ----------

## A left-anti join returns only rows from the left table that are not matching any rows from the right table.
## If the hash is different per id, the row is different - compared to the target.
df_rows_to_update = (
    df_updates.alias("source")
    .where("is_current = true")
    .join(df_target_dim_employee.alias("target"), ["id", "hash", "is_current"], "leftanti")
    .orderBy(F.col("source.id"))
)

df_rows_to_update = df_updates
display(df_rows_to_update)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expire old rows in the target
# MAGIC ... if new rows for the ID are coming from the source.

# COMMAND ----------

from delta.tables import *

target_dim_employee = DeltaTable.forName(spark, target_dim_employee_tablename)
target_dim_employee.alias("target").merge(
    source=df_rows_to_update.alias("updates"), condition="target.id = updates.id"
).whenMatchedUpdate(
    condition="target.is_current = True AND target.hash <> updates.hash",  # Invalidate only the current row and only if any value (hash) changed
    set={"is_current": F.lit(False), "valid_to": F.lit(F.current_timestamp())},
).execute()

display(
    spark.sql(
        f"SELECT * FROM {target_dim_employee_tablename} ORDER BY id, surrogate_id"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert new rows

# COMMAND ----------

df_rows_to_insert = (df_rows_to_update
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit(None).cast("date"))
)

df_rows_to_insert.write.format("delta").mode("append").saveAsTable(target_dim_employee_tablename)
display(spark.sql(f"SELECT * FROM {target_dim_employee_tablename} ORDER BY id, surrogate_id"))
