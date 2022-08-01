# Databricks notebook source
# MAGIC %md
# MAGIC # Pandas
# MAGIC 
# MAGIC Library for working with tabular data and perform all parts of the analysis from collection and manipulation through aggregation and visualization.

# COMMAND ----------

# MAGIC %pip install pandas

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame
# MAGIC - data in table representation
# MAGIC - Pandas supports many formats (csv, excel, sql, json, parquet,…)

# COMMAND ----------

df = pd.read_csv("default.titanicdata_csv.csv")
df
