# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
username

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS username

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE username

# COMMAND ----------

source_tips = "dbfs:/FileStore/python-workshop/tips.csv"
source_iris = "dbfs:/FileStore/python-workshop/iris.csv"
source_air_quality_no2 = "dbfs:/FileStore/python-workshop/air_quality_pm25_long.csv"
source_air_quality_pm25 = "dbfs:/FileStore/python-workshop/air_quality_no2_long.csv"

tips = spark.read.option("header", "true").csv(source_tips)
iris = spark.read.option("header", "true").csv(source_iris)
air_quality_no2 = spark.read.option("header", "true").csv(source_air_quality_no2)
air_quality_pm25 = spark.read.option("header", "true").csv(source_air_quality_pm25)

tips.write.mode("overwrite").saveAsTable("tips_table")
iris.write.mode("overwrite").saveAsTable("iris_table")
air_quality_no2.write.mode("overwrite").saveAsTable("air_quality_no2")
air_quality_pm25.write.mode("overwrite").saveAsTable("air_quality_pm25")
