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
filtered_passengers = df_titanic[(df_titanic["Age"] > 35) & (df_titanic["Sex"] == "female")]
filtered_passengers

# COMMAND ----------

# DBTITLE 1,Remove NaN values and IN
# SQL: SELECT * FROM df_titanic WHERE Cabin IS NOT NULL Embarked IN (C, S)
df_titanic[(df_titanic["Cabin"].notna()) & (df_titanic["Embarked"].isin(["C", "S"]))]

# COMMAND ----------

# DBTITLE 1,Access specific rows and columns by condition
adult_women = df_titanic.loc[df_titanic["Age"] > 18, ["Name", "Age"]]
adult_women

# COMMAND ----------

# DBTITLE 1,Access specific cells by index
df_titanic.iloc[0:25, 0:4]

# COMMAND ----------

# DBTITLE 1,Add new column
df_titanic["Adult"] = df_titanic["Age"] > 18
df_titanic.head(30)

# COMMAND ----------

# DBTITLE 1,Rename columns
df_renamed = df_titanic.rename(columns={"Parch": "Parents", "SibSp": "Siblings"})
df_renamed

# COMMAND ----------

# DBTITLE 1,Combine data from multiple tables
pd.concat([air_quality_pm25, air_quality_no2], keys=["PM25", "NO2"])

# COMMAND ----------

# DBTITLE 1,Join tables on common attribute
pd.merge(air_quality, stations_coord, how="left", on="location")

# COMMAND ----------

# DBTITLE 1,Aggregating statistics
titanic["Age"].mean()

# COMMAND ----------

titanic[["Age", "Fare"]].median()

# COMMAND ----------

titanic[["Age", "Fare"]].describe()

# COMMAND ----------

titanic.agg(
    {
        "Age": ["min", "max", "median", "skew"],
        "Fare": ["min", "max", "median", "mean"],
    }
)

# COMMAND ----------

# DBTITLE 1,Group by

