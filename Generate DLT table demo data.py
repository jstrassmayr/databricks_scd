# Databricks notebook source
# MAGIC %md
# MAGIC # Initial source data
# MAGIC
# MAGIC - Create a source table *dlt_employee*
# MAGIC - Insert one row
# MAGIC - Enable the Change Data Feed
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.scd_example;
# MAGIC USE main.scd_example;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dlt_employee (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   sys_edit_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO dlt_employee VALUES (33, "John", 10, CAST('2023-01-01' AS TIMESTAMP));
# MAGIC
# MAGIC SELECT * from dlt_employee;

# COMMAND ----------

# MAGIC %md
# MAGIC # Update John to age=11 and insert Mary and Ben

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dlt_employee SET age = 11, sys_edit_dt = '2024-11-15' WHERE id = 33;
# MAGIC
# MAGIC INSERT INTO dlt_employee VALUES
# MAGIC (44, 'Mary', 20, '2024-11-15'),
# MAGIC (55, 'Ben', 30, '2024-11-15')
# MAGIC ;
# MAGIC SELECT * FROM dlt_employee ORDER BY id, sys_edit_dt;

# COMMAND ----------

# MAGIC %md
# MAGIC # Update John to age=70

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dlt_employee SET age = 70, sys_edit_dt = '2026-11-15' WHERE id = 33;
# MAGIC
# MAGIC SELECT * FROM dlt_employee ORDER BY id, sys_edit_dt;

# COMMAND ----------

# MAGIC %md
# MAGIC # Insert Mike

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dlt_employee VALUES
# MAGIC (66, 'Mike', 40, '2028-11-15')
# MAGIC ;
# MAGIC SELECT * FROM dlt_employee ORDER BY id, sys_edit_dt;

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Mary to age=25

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dlt_employee SET age = 25, sys_edit_dt = '2030-11-15' WHERE id = 44;
# MAGIC
# MAGIC SELECT * FROM dlt_employee ORDER BY id, sys_edit_dt;
