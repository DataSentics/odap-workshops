# Databricks notebook source
# MAGIC %run ./Setup-SQL

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta format
# MAGIC 
# MAGIC - time travel/historization
# MAGIC - schema evolution
# MAGIC - caching
# MAGIC - MERGE operation
# MAGIC 
# MAGIC [Introduction](https://docs.microsoft.com/en-us/azure/databricks/delta/delta-intro)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/FileStore/python-workshop/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manually create Delta table from CSV

# COMMAND ----------

df = spark.read.option("header", "true").csv(
    "file:/dbfs/FileStore/python-workshop/tips.csv"
)

(
    df.write.mode("overwrite")
    .partitionBy("day")
    .format("delta")
    .saveAsTable("tips")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select from Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now we can query the delta table as SQL table
# MAGIC SELECT *
# MAGIC FROM tips;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tips WHERE size = 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED tips

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tips

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{username}.db/tips")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{username}.db/tips_from_delta/_delta_log")

# COMMAND ----------

display(
    spark.read.format("json").load(
        "dbfs:/FileStore/dbx-workshop/tips/_delta_log/00000000000000000000.json"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE tips TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge

# COMMAND ----------

spark.createDataFrame(
    [
        [20.65, 3.35, "Male", "No", "Sat", "Dinner", 3, None, True],
        [9.78, 1.73, "Male", "No", "Thur", "Lunch", 2, 12.2, False],
        [1.11, 0.11, "Male", "No", "Thur", "Lunch", 2, None, False],
    ],
    schema="total_bill double,tip double,sex string,smoker string,day string,time string,size string, new_total_bill double, delete_row boolean",
).createOrReplaceTempView("tips_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tips as target USING tips_update as source ON source.total_bill = target.total_bill
# MAGIC and source.tip = target.tip
# MAGIC and source.sex = target.sex
# MAGIC and source.smoker = target.smoker
# MAGIC and source.time = target.time
# MAGIC and source.size = target.size
# MAGIC when matched
# MAGIC and source.delete_row then delete
# MAGIC when matched then
# MAGIC update
# MAGIC set
# MAGIC   target.total_bill = source.new_total_bill
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tips where total_bill = 20.65
