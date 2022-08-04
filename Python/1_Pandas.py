# Databricks notebook source
# MAGIC %md
# MAGIC # Pandas
# MAGIC * [Documentation](https://pandas.pydata.org/docs/)
# MAGIC * Library for working with tabular data and perform all parts of the analysis from collection and manipulation through aggregation and visualization.

# COMMAND ----------

# DBTITLE 1,Create SQL tables
# MAGIC %run ./Setup-SQL

# COMMAND ----------

import pandas as pd

# COMMAND ----------

source_tips = "file:/dbfs/FileStore/python-workshop/tips.csv"
source_iris = "file:/dbfs/FileStore/python-workshop/iris.csv"
source_air_quality_no2 = "file:/dbfs/FileStore/python-workshop/air_quality_pm25_long.csv"
source_air_quality_pm25 = "file:/dbfs/FileStore/python-workshop/air_quality_no2_long.csv"

df_iris = pd.read_csv(source_iris)
air_quality_pm25 = pd.read_csv(source_air_quality_pm25)
air_quality_no2 = pd.read_csv(source_air_quality_no2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame
# MAGIC - data in table representation
# MAGIC - Pandas supports many formats (csv, excel, sql, json, parquet,…)

# COMMAND ----------

# DBTITLE 1,Read CSV to DataFrame
df_tips = pd.read_csv("file:/dbfs/FileStore/python-workshop/tips.csv")
df_tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display first N rows

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * FROM tips_table LIMIT 5

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display schema

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display size

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
# how to find table size (nrows, ncolumns)
df_tips.shape

# COMMAND ----------

# DBTITLE 1,Write CSV file from DataFrame
# df_tips.to_csv("file:/dbfs/FileStore/python-workshop/tips_new.csv")
# df_from_csv = pd.read_csv(f"file:/dbfs/FileStore/python-workshop/tips_new.csv")
# df_from_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subsets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get one column

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT total_bill FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
total = df_tips["total_bill"]
total

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get subset of columns

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT tip, day, time FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
# get new DataFrame with subset of columns
tips_daytime = df_tips[["tip", "day", "time"]]
tips_daytime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter rows

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * FROM tips_table WHERE tip > 3 AND time == "Lunch"

# COMMAND ----------

# DBTITLE 1,Pandas
# select files where is condition True
# allowed operations (>, <, ==, >=, <=, !=, .isin([]), .notna()...), logical operators (|, &)
filtered_tips = df_tips[(df_tips["tip"] > 3) & (df_tips["time"] == "Lunch")]
filtered_tips

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter NaN values and IN

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * FROM tips_table WHERE total_bill IS NOT NULL AND day IN ("Sun", "Sat")

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[(df_tips["total_bill"].notna()) & (df_tips["day"].isin(["Sun", "Sat"]))]

# COMMAND ----------

# DBTITLE 0,Pandas Drop NaN
# add NaN value
df = pd.read_csv(source_tips)
df.iloc[0, 0] = None

# COMMAND ----------

# DBTITLE 0,Check null values
# check NaN values
df.isnull().any()

# COMMAND ----------

# drop NaN values
df.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select specific rows and columns by condition

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT day, time, size FROM tips_table WHERE size > 2

# COMMAND ----------

# DBTITLE 1,Pandas
families = df_tips.loc[df_tips["size"] > 2, ["day", "time", "size"]]
families

# COMMAND ----------

# DBTITLE 1,Pandas
# Access specific rows and columns by index
df_tips.iloc[0:25, 0:4]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new column

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT total_bill, tip, sex, smoker, day, time, size, CASE
# MAGIC       WHEN size > 2 THEN true
# MAGIC       ELSE false
# MAGIC    END AS family
# MAGIC    FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips["family"] = df_tips["size"] > 2
df_tips.head(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename column

# COMMAND ----------

# DBTITLE 1,Pandas
df_renamed = df_tips.rename(columns={"time": "meal_type"})
df_renamed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine data from multiple tables

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC         *
# MAGIC     FROM
# MAGIC        air_quality_pm25
# MAGIC UNION
# MAGIC     SELECT
# MAGIC         *
# MAGIC     FROM
# MAGIC         air_quality_no2

# COMMAND ----------

# DBTITLE 1,Pandas
pd.concat([air_quality_pm25, air_quality_no2])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join two tables

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT * FROM air_quality_pm25 RIGHT JOIN air_quality_no2 ON "air_quality_pm25.date.utc"="air_quality_no2.date_utc" AND air_quality_pm25.location=spark_catalog.username.air_quality_no2.location

# COMMAND ----------

# DBTITLE 1,Pandas
no2 = air_quality_no2[["date.utc", "location", "value", "unit"]].rename(
  columns={"value": "no2", "unit": "unit_no2"}
)
pm25 = air_quality_pm25.rename(columns={"value": "pm25", "unit": "unit_pm25"})
pd.merge(pm25, no2, how="left", on=["location", "date.utc"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregating statistics

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT MEAN(tip) FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips["tip"].mean()

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT MEAN(size), MEAN(tip) FROM tips_table

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[["size", "tip"]].mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[["size", "tip"]].describe()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.agg(
  {
    "size": ["min", "max", "median"],
    "tip": ["min", "max", "median", "mean"],
  }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group by

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC SELECT time, MEAN(tip)
# MAGIC FROM tips_table
# MAGIC GROUP BY time

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips[["time", "tip"]].groupby("time").mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.groupby("time").mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips.groupby(["day", "time"]).mean()

# COMMAND ----------

# DBTITLE 1,Pandas
df_tips["day"].value_counts()

# COMMAND ----------

# DBTITLE 1,Pandas
df_iris

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic visualizations

# COMMAND ----------

# DBTITLE 1,Pandas
# Plot
df_iris.plot(figsize=(10, 6))

# COMMAND ----------

# DBTITLE 1,Pandas
# Box-plot
df_iris.plot.box(figsize=(10, 6))

# COMMAND ----------

# DBTITLE 1,Pandas
# Scatter-plot
colors = {"Iris-setosa": "red", "Iris-versicolor": "green", "Iris-virginica": "blue"}
df_iris.plot.scatter(
  "PetalLength", "PetalWidth", c=df_iris["Name"].map(colors), figsize=(10, 6)
)
