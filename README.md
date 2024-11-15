# Slowly Changing Dimensions Type 2

This repo demos how to handle Slowly Changing Dimension in Databricks
- with regular Delta Tables (see [Notebook](https://github.com/jstrassmayr/databricks_scd/blob/main/SCD%20with%20regular%20Delta%20Tables.py)) and
- with Delta Live Tables (see [Notebook](https://github.com/jstrassmayr/databricks_scd/blob/main/SCD%20with%20Delta%20Live%20Tables.py))

It contains a Notebook with a step-by-step approach for each technology and a [demo Excel file](https://github.com/jstrassmayr/databricks_scd/blob/main/SCD%20example%20data.xlsx) for understanding source and target data.

## Delta Tables and the MERGE INTO command

You can upsert data from a source table, view, or DataFrame into a target Delta table by using the [MERGE](https://docs.databricks.com/en/delta/merge.html#language-python) operation. 
Delta Lake supports inserts, updates, and deletes in MERGE, and it supports extended syntax beyond the SQL standards to facilitate advanced use cases.

## Syntax
```
MERGE INTO target
USING source
ON source.key = target.key
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
```

## Delta Live Tables and the APPLY CHANGES command

Delta Live Tables simplifies change data capture (CDC) with the [APPLY CHANGES and APPLY CHANGES FROM SNAPSHOT APIs](https://docs.databricks.com/en/delta-live-tables/cdc.html).

## Syntax
```
import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def users():
  return spark.readStream.table("cdc_data.users")

dlt.create_streaming_table("target")

dlt.apply_changes(
  target = "target",
  source = "users",
  keys = ["userId"],
  sequence_by = col("sequenceNum"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "2"
)
```
