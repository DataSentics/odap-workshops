# Databricks notebook source
# MAGIC %md
# MAGIC # Pandas
# MAGIC 
# MAGIC Library for working with tabular data and perform all parts of the analysis from collection and manipulation through aggregation and visualization.

# COMMAND ----------

# MAGIC %pip install pandas
# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame
# MAGIC - data in table representation
# MAGIC - Pandas supports many formats (csv, excel, sql, json, parquet,…)

# COMMAND ----------

# DBTITLE 1,Read CSV to DataFrame
df_titanic = pd.read_csv("/dbfs/FileStore/user/ondrej.lostak@datasentics.cz/TitanicData.csv")
df_titanic

# COMMAND ----------

# DBTITLE 1,Display first N rows
df_titanic.head(5)

# COMMAND ----------

# DBTITLE 1,Display schema
# df_titanic.dtypes
df_titanic.dtypes

# COMMAND ----------

# DBTITLE 1,Display size
# how to find table size (nrows, ncolumns)
df_titanic.shape

# COMMAND ----------

# DBTITLE 1,Write CSV file from DataFrame
df_titanic.to_csv("/dbfs/FileStore/user/ondrej.lostak@datasentics.cz/titanic.csv")
df_from_csv = pd.read_csv("/dbfs/FileStore/user/ondrej.lostak@datasentics.cz/titanic.csv")
df_from_csv

# COMMAND ----------

# DBTITLE 1,Task 1
# MAGIC %md
# MAGIC 1. read file from "source_file" as pandas DataFrame
# MAGIC 2. display first 10 rows of the DataFrame
# MAGIC 3. display schema of the DataFrame
# MAGIC 4. find number of rows and columns
# MAGIC 4. write the DataFrame to new csv file "/dbfs/FileStore/user/ondrej.lostak@datasentics.cz/TitanicData.csv"

# COMMAND ----------

source_path = "/dbfs/FileStore/user/ondrej.lostak@datasentics.cz/TitanicData.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subsets

# COMMAND ----------

# DBTITLE 1,Get one column
# get one column
ages = df_titanic["Age"]
ages

# COMMAND ----------

# DBTITLE 1,Get subset of columns
# get new DataFrame with subset of columns
# SQL: SELECT Name, Sex, Age FROM titanic 
passengers = df_titanic[["Name", "Sex", "Age"]]
passengers

# COMMAND ----------

# DBTITLE 1,Filter rows
# select files where is condition True
# allowed operations (>, <, ==, >=, <=, !=, .isin([]), .notna()...), logical operations (|, &)
# SQL: SELECT * FROM df_titanic WHERE Age > 35 AND Sex == "female"
filtered_passengers = passengers[(passengers["Age"] > 35) & (passengers["Sex"] == "female")]
filtered_passengers
