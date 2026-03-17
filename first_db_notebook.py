# Databricks notebook source

# COMMAND ----------

print("Hello from Databricks!")

# COMMAND ----------

df = spark.range(10)
df.show()