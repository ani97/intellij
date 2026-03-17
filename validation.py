# Databricks notebook source

# COMMAND ----------

# Create dummy dataframe
data = [(1, "Alice", 30), (2, "Bob", 25), (3, None, 22)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()

# COMMAND ----------

# Check expected columns exist
expected_columns = ["id", "name", "age"]

for col in expected_columns:
    if col in df.columns:
        print(f"✅ {col} exists")
    else:
        print(f"❌ {col} is MISSING")

# COMMAND ----------

# Check for nulls
from pyspark.sql.functions import col

for column in expected_columns:
    null_count = df.filter(col(column).isNull()).count()
    if null_count == 0:
        print(f"✅ {column} has no nulls")
    else:
        print(f"❌ {column} has {null_count} null(s)")