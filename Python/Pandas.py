# Databricks notebook source
# MAGIC %md
# MAGIC # Pandas
# MAGIC * [Documentation](https://pandas.pydata.org/docs/)
# MAGIC * Library for working with tabular data and perform all parts of the analysis from collection and manipulation through aggregation and visualization.

# COMMAND ----------

import pandas as pd

# COMMAND ----------

source_tips = "./data/tips.csv"
source_iris = "./data/iris.csv"
source_air_quality_no2 = "./data/air_quality_pm25_long.csv"
source_air_quality_pm25 = "./data/air_quality_no2_long.csv"

df_iris = pd.read_csv(source_iris, index=False)
air_quality_pm25 = pd.read_csv(source_air_quality_pm25, index=False)
air_quality_no2 = pd.read_csv(source_air_quality_no2, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame
# MAGIC - data in table representation
# MAGIC - Pandas supports many formats (csv, excel, sql, json, parquet,…)

# COMMAND ----------

# DBTITLE 1,Read CSV to DataFrame
df_tips = pd.read_csv("./data/tips.csv")
df_tips

# COMMAND ----------

# DBTITLE 1,Display first N rows
df_tips.head(5)

# COMMAND ----------

# DBTITLE 1,Display schema
df_tips.dtypes

# COMMAND ----------

# DBTITLE 1,Display size
# how to find table size (nrows, ncolumns)
df_tips.shape

# COMMAND ----------

# DBTITLE 1,Write CSV file from DataFrame
df_tips.to_csv("./data/tips_new.csv")
df_from_csv = pd.read_csv("./data/tips_new.csv")
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

# MAGIC %md
# MAGIC ## Subsets

# COMMAND ----------

# DBTITLE 1,Get one column
# get one column
total = df_tips["total_bill"]
total

# COMMAND ----------

# DBTITLE 1,Get subset of columns
# get new DataFrame with subset of columns
# SQL: SELECT tip, day, time FROM df_tips 
tips_daytime = df_tips[["tip", "day", "time"]]
tips_daytime

# COMMAND ----------

# DBTITLE 1,Filter rows
# select files where is condition True
# allowed operations (>, <, ==, >=, <=, !=, .isin([]), .notna()...), logical operations (|, &)
# SQL: SELECT * FROM df_tips WHERE tip > 3 AND time == "Lunch"
filtered_tips = df_tips[(df_tips["tip"] > 3) & (df_tips["time"] == "Lunch")]
filtered_tips

# COMMAND ----------

# DBTITLE 1,Filter NaN values and IN
# SQL: SELECT * FROM df_titanic WHERE Cabin IS NOT NULL Embarked IN (C, S)
df_tips[(df_tips["total_bill"].notna()) & (df_tips["day"].isin(["Sun", "Sat"]))]

# COMMAND ----------

# DBTITLE 1,Drop NaN
df = pd.read_csv(source_tips)
df.iloc[0,0] = None

# COMMAND ----------

# DBTITLE 1,Check null values
df.isnull().any()

# COMMAND ----------

df.dropna()

# COMMAND ----------

# DBTITLE 1,Access specific rows and columns by condition
families = df_tips.loc[df_tips["size"] > 2, ["day", "time", "size"]]
families

# COMMAND ----------

# DBTITLE 1,Access specific cells by index
df_tips.iloc[0:25, 0:4]

# COMMAND ----------

# DBTITLE 1,Add new column
df_tips["family"] = df_tips["size"] > 2
df_tips.head(30)

# COMMAND ----------

# DBTITLE 1,Rename columns
df_renamed = df_tips.rename(columns={"time": "meal_type"})
df_renamed

# COMMAND ----------

# DBTITLE 1,Combine data from multiple tables
pd.concat([air_quality_pm25, air_quality_no2])

# COMMAND ----------

# DBTITLE 1,Join tables on common attribute
no2 = air_quality_no2[["date.utc", "value", "unit"]].rename(columns={"value": "no2", "unit": "unit_no2"})
pm25 = air_quality_pm25.rename(columns={"value": "pm25", "unit": "unit_pm25"})
pd.merge(pm25, no2, how="left", on="date.utc")

# COMMAND ----------

# DBTITLE 1,Aggregating statistics
df_tips["tip"].mean()

# COMMAND ----------

df_tips[["size", "tip"]].median()

# COMMAND ----------

df_tips[["size", "tip"]].describe()

# COMMAND ----------

df_tips.agg(
    {
        "size": ["min", "max", "median"],
        "tip": ["min", "max", "median", "mean"],
    }
)

# COMMAND ----------

# DBTITLE 1,Group by
df_tips[["time", "tip"]].groupby("time").mean()

# COMMAND ----------

df_tips.groupby("time").mean()

# COMMAND ----------

df_tips.groupby(["day", "time"]).mean()

# COMMAND ----------

df_tips["day"].value_counts()

# COMMAND ----------

df_iris

# COMMAND ----------

# DBTITLE 1,Basic plot
df_iris.plot(figsize=(10,6))

# COMMAND ----------

# DBTITLE 1,Boxplot
df_iris.plot.box(figsize=(10,6))

# COMMAND ----------

df_iris["Name"].unique()

# COMMAND ----------

# DBTITLE 1,Scatter-plot
colors = {'Iris-setosa':'red', 'Iris-versicolor':'green', 'Iris-virginica':'blue'}
df_iris.plot.scatter('SepalLength', 'SepalWidth', c=df_iris['Name'].map(colors), figsize=(10,6))
