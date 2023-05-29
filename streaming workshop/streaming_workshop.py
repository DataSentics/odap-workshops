# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Structured Streaming Workshop
# MAGIC
# MAGIC Content:
# MAGIC 1) Triggers
# MAGIC 2) Stateless operations
# MAGIC 3) Checkpoints
# MAGIC 4) Statefull operations
# MAGIC 5) Watermarking
# MAGIC 6) Metrics

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as T
import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from datetime import datetime

# COMMAND ----------

# creates tables and generates artificial stream
%run "./generate_artificial_stream"

# COMMAND ----------

# get name of your catalog
catalog_name = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .split("@")[0]
)
catalog_name = catalog_name.replace("-", "_")
catalog_name

# COMMAND ----------

# read streaming table as static table
tips_static = spark.read.table(f"{catalog_name}.kafka_workshop.tips")
tips_static.count()

# COMMAND ----------

# read streaming table as stream
tips_stream = spark.readStream.table(f"{catalog_name}.kafka_workshop.tips")
display(tips_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stateless operations
# MAGIC
# MAGIC * filter
# MAGIC * projection/select
# MAGIC * udf
# MAGIC * forEachBatch
# MAGIC * join with static table

# COMMAND ----------

# filter
tips_stream_lunch = tips_stream.filter(f.col("time") == "Lunch")

# udf
@udf
def to_upper(s):
    if s is not None:
        return s.upper()


# select
tips_stream_lunch = tips_stream_lunch.select(
    "timestamp", "restaurant_id", "total_bill", to_upper("day").alias("day"), "time"
)
display(tips_stream_lunch)

# COMMAND ----------

# join static table
restaurants = spark.read.table(f"{catalog_name}.kafka_workshop.restaurants")

restaurant_incomes = tips_stream_lunch.join(
    restaurants, tips_stream_lunch.restaurant_id == restaurants.id
).drop("id")

display(restaurant_incomes)

# COMMAND ----------

restaurants.persist()


def update_restaurants(batch_df, batch_id):
    batch_df.unpersist()
    batch_df = spark.read.table(f"{catalog_name}.kafka_workshop.restaurants")
    batch_df.persist()
    print(f"Refreshing static restaurants")


restaurants_clock = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
    .selectExpr("CAST(value as LONG) as trigger")
)

# Update table each 2 hours
(
    restaurants_clock.writeStream.outputMode("append")
    .foreachBatch(update_restaurants)
    .queryName("RefreshRestaurants")
    .trigger(processingTime="10 minutes")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write stream and checkpoints
# MAGIC
# MAGIC #### Checkpoints
# MAGIC
# MAGIC A checkpoint helps build fault-tolerant and resilient Spark applications. In Spark Structured Streaming, it maintains intermediate state on HDFS compatible file systems to recover from failures. To specify the checkpoint in a streaming query, we use the checkpointLocation parameter.

# COMMAND ----------

(
    restaurant_incomes.writeStream.option(
        "checkpointLocation",
        f"/tmp/kafka_workshop/{catalog_name}/restaurant_lunch_incomess/_checkpoints/",
    ).toTable(f"{catalog_name}.kafka_workshop.restaurant_lunch_incomes")
)

# COMMAND ----------

dbutils.fs.ls("/tmp/kafka_workshop/restaurant_lunch_incomess/_checkpoints/")

# COMMAND ----------

schema = T.StructType(
    [
        T.StructField("restaurant_id", T.IntegerType(), True),
        T.StructField("income", T.DoubleType(), True),
    ]
)

spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.kafka_workshop.incomes"
)


def func(batch_df, batch_id):
    batch_df = batch_df.groupBy("restaurant_id").agg(
        f.sum("total_bill").alias("income")
    )
    (
    batch_df.merge(
        source = batch_df.alias("updates"),
        condition = "events.eventId = updates.eventId"
    ).whenNotMatchedInsertAll()
    .execute()


(
    tips_stream_lunch.writeStream.foreachBatch(func)
    .outputMode("update")
    .option(
        "checkpointLocation", f"/tmp/kafka_workshop/{catalog_name}/income/_checkpoints"
    )
    .start()
)

# COMMAND ----------

incomes = spark.read.table(f"{catalog_name}.kafka_workshop.incomes")
display(incomes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statefull operations
# MAGIC * window operations
# MAGIC * dropDuplicates
# MAGIC * flatMapGroupWhiteState
# MAGIC * Stream-stream joins

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermarking
# MAGIC
# MAGIC Watermarking in structured streaming is a technique that ensures the correctness and consistency of event time processing by defining a threshold for late-arriving events.
# MAGIC
# MAGIC * The watermark specifies the point after which Spark assumes there will be no more data with earlier timestamps.
# MAGIC * Watermarking is typically used with window-based operations like aggregations or joins.
# MAGIC * It prevents issues such as unbounded memory usage and incorrect aggregations due to late data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows opearations
# MAGIC
# MAGIC Windows are a mechanism for calculating aggregations over subsets of data specified by a time range.
# MAGIC
# MAGIC "Don't use aggregation over all data!!! It is not computationally possible."
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ![image](url)
# MAGIC
# MAGIC Example with watermarking:
# MAGIC
# MAGIC ![image](url)
# MAGIC
# MAGIC window function parameters:
# MAGIC * timeColumn
# MAGIC * windowDuration
# MAGIC * slideDuration=None
# MAGIC * startTime=None

# COMMAND ----------

sum_of_bills = (
    tips_stream.withWatermark("timestamp", "10 minutes")
    .groupBy(
        f.window(tips_stream.timestamp, "10 minutes", "5 minutes"),
        tips_stream.restaurant_id,
    )
    .agg(f.sum(tips_stream.total_bill).alias("income"))
)

display(sum_of_bills)

# COMMAND ----------

# MAGIC %md
# MAGIC #### dropDuplicates
# MAGIC
# MAGIC The example below adds duplicates to the stream.
# MAGIC
# MAGIC 1) run it first with the duplicates
# MAGIC 2) clear checkpoints
# MAGIC 3) uncommend dropDuplicates and rerun the cell

# COMMAND ----------

# dropDuplicates

# create artificial duplicates
duplicates = (
    tips_stream.withColumn("drop", f.when(f.rand() > 0.8, False).otherwise(True))
    .filter(f.col("drop"))
    .drop("drop")
)

tips_with_duplicates = tips_stream.unionByName(duplicates)

# tips_with_duplicates = tips_with_duplicates.withWatermark("timestamp", "10 minutes").dropDuplicates()

(
    tips_with_duplicates.writeStream.option(
        "checkpointLocation",
        f"/tmp/kafka_workshop/{catalog_name}/duplicites/_checkpoints/",
    ).toTable(f"{catalog_name}.kafka_workshop.duplicites")
)

# COMMAND ----------

df = spark.read.table(f"{catalog_name}.kafka_workshop.duplicites")
print(f"Duplicits: {(df.count() - df.dropDuplicates().count())}")

# COMMAND ----------

dbutils.fs.rm(f"/tmp/kafka_workshop/{catalog_name}/duplicites/_checkpoints/", True)
spark.sql(f"DROP TABLE {catalog_name}.kafka_workshop.duplicites")

# COMMAND ----------

# flatMapGroupWhiteState
from typing import Tuple, Iterator


def func(
    key: Tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    if state.hasTimedOut:
        (restaurant_id,) = key
        (count,) = state.get
        state.remove()
        yield pd.DataFrame({"restaurant": [restaurant_id], "count": [count]})
    else:
        # Aggregate the number of words.
        count = sum(map(lambda pdf: len(pdf), pdfs))
        if state.exists:
            (old_count,) = state.get
            count += old_count
        state.update((count,))
        # Set the timeout as 10 seconds.
        state.setTimeoutDuration(10000)
        yield pd.DataFrame()


# Group the data by word, and compute the count of each group
output_schema = "restaurant LONG, count LONG"
state_schema = "count LONG"
restaurants = (
    tips_stream.groupBy(f.col("restaurant_id")).applyInPandasWithState(
    func,
    output_schema,
    state_schema,
    "append",
    GroupStateTimeout.ProcessingTimeTimeout,
    )
    .withColumn("timestamp", f.lit(datetime.now()))
)

"""(
    restaurants.writeStream
    .outputMode("update")
    .option(
        "checkpointLocation",
        f"/tmp/kafka_workshop/{catalog_name}/restaurants_frequency/_checkpoints/",
    ).toTable(f"{catalog_name}.kafka_workshop.restaurants_frequency")
)"""
display(restaurants)

# COMMAND ----------

tips_stream_withWatermark = tips_stream.withWatermark("timestamp", "10 seconds")
restaurants_withWatermark = restaurants.withWatermark("timestamp", "10 seconds")

tips_count = tips_stream_withWatermark.join(restaurants_withWatermark, f.expr("restaurant_id = restaurant") )
display(tips_count)
