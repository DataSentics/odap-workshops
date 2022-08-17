# Databricks notebook source
# MAGIC %md
# MAGIC <div style="width: 100%; background: black; height: 96px">
# MAGIC   <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/db_learning_rev.png" style="display: block; margin: auto"/>
# MAGIC </div>
# MAGIC # [EXERCISE] Spark Workshop
# MAGIC ## PART 2 - (py)spark SQL module

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) 1 Import

# COMMAND ----------

# Import only what you need
#from pyspark.sql.functions import col, lit, collect_list #...

# Or (imho better), whole modules
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) 2 Read data

# COMMAND ----------

# Gapminder dataset
df_gapminder = (
    spark
    .table('demo.gapminder_basic_stats')
)

# Nobel dataset
df_nobel = (
    spark
    .table('demo.nobel_prize')
)

# COMMAND ----------

display(df_gapminder)

# COMMAND ----------

display(df_nobel)

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) 3 Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.0 [EXERCISE] PySpark SQL/DataFrame functions
# MAGIC Difficulty 3/10 - 10min

# COMMAND ----------

# DBTITLE 1,Quest: various operations
# QUEST: Take the Gapminder dataset and go through the following tasks:
# 1. Get the "summary" of the dataframe
# 2. Get maximum and minimum of the `pop` column for `continent == "Asia"`
# 3. Apply `log` function on the `year` column
# 4. Create a new column `adv_col` and let it equal to either 1 (if lifeExp > 50) or 0 (if lifeExp <= 50)
# 5. BONUS: calculate correlation between `year` and `pop`

# COMMAND ----------

# -----------------
#      ANSWER
# -----------------

# 1. Get the "summary" of the dataframe
df_gapminder.summary()

# 2. Count maximum and minimum of the `pop` column
df_gapminder.filter(f.col('country') == 'China').agg(f.max('pop'), f.min('pop'))

# 3. Apply `log` function on the `year` column
df_gapminder.select(f.log('year'))

# 4. Create a new column `adv_col` which equals to either 1 (if lifeExp > 50) or 0 (if lifeExp <= 50)
df_gapminder.withColumn('adv_col', f.when(f.col('lifeExp') > 50, 1).otherwise(0)) # good solution
df_gapminder.withColumn('adv_col', (f.col('lifeExp') > 50).cast('int')) # but this is more concise :)

# 5. BONUS: calculate correlation between `year` and `pop`
df_gapminder.corr('pop', 'year')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 [EXERCISE] Aggregations
# MAGIC Difficulty 8/10 - 10 min, then help

# COMMAND ----------

# DBTITLE 1,Quest: prize sharing per category
# Nobel dataset
# QUEST: Find which category has the largest average number of laurates sharing the prize

# Hint: there is a pyspark.sql.function for array length

# COMMAND ----------

# -----------------
#      ANSWER
# -----------------

df_laur_stat = (
    df_nobel
    .withColumn('nlaureates', f.size('laureates'))
    .groupBy('category')
    .agg(
        f.avg('nlaureates').alias('nlaureates_avg')
    )
    .orderBy(f.desc('nlaureates_avg'))
)

display(df_laur_stat)

# COMMAND ----------

# DBTITLE 1,Quest: life exp increase
# Gapminder Dataset: Recall that in the Gapminder dataset, there are measurements of `lifeExp`ectancy. They are measured in 5-year periods (1952, 1957, 1962, ...), for each country. 
# QUEST: Find which country had the highest increase in life expectancy between any two consecutive "5-years". 


# Hint: 
# - pyspark.sql.window.Window, f.lag 
# - (OR) self join. 
# - ...

# COMMAND ----------

# -----------------
#      ANSWER
# -----------------

# Window function
w = Window().partitionBy('country').orderBy('year')

display(df_gapminder
        .withColumn('lifeExp_lag', f.lag('lifeExp').over(w))
        .withColumn('lifeExp_diff', f.col('lifeExp') - f.col('lifeExp_lag'))
        .sort('lifeExp_diff', ascending=False)
        .select('country', 'year', 'lifeExp_diff'))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 [EXERCISE] UDFs
# MAGIC Difficulty 4/10 - 5 min

# COMMAND ----------

# DBTITLE 1,Quest: UDF replacing letters
# QUEST: Create UDF that replaces all 'a' letters with 'L' letter. Apply it to the Gapminder dataset (to the column `country`).  E.g. "Afghanistan" will become "AfghLnistLn"
# Hint: Python native function: string.replace()

# Bonus question: could this be done via SparkSQL function? 

# COMMAND ----------

# -----------------
#      ANSWER
# -----------------

# Define UDF
@udf(T.StringType())
def uppercase_a(x_string):
  return x_string.replace('a', 'L')

display(
    df_gapminder
    .select('country', uppercase_a(f.col('country')).alias('country_replaced'))
)

# Answer to the question: Yes! Via the `regexp_replace` function

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 [EXERCISE] Complex type manipulation 
# MAGIC (Difficulty: 9/10) - 15min, then help

# COMMAND ----------

# DBTITLE 1,Quest: array, struct manipulation
# QUEST: Take the Nobel dataframe and create an identical dataframe with just one difference - in the `laureates` column, keep ONLY the "firstname" attribute in the structs and discard the rest (=> no "id", "motivation", "share" or "surname" in the structs of the `laureates` array). 
# That is, the resulting display should look like
# +--------+-----------------+----+-------------------------------------------------------------------------------------------+
# |category|overAllMotivation|year|laureates                                                                                  |
# +--------+-----------------+----+-------------------------------------------------------------------------------------------+
# |medicine|null             |1950|[{"firstname": "Edward Calvin}, {"firstname": "Tadeus"}, {"firstname": "Philip Showalter"}]|
# |peace   |null             |1908|[{"firstname": "Klas Pontus}, {"firstname": "Fredrik"}]                                    |
# +--------+-----------------+----+-------------------------------------------------------------------------------------------+

# Hints: 
# - split+tranform+combine approach; with `struct`, `collect_list` functions
# - (or use udf)

# COMMAND ----------

# -----------------
#      ANSWER
# -----------------

# Split-transform-combine approach
display(
    df_nobel
    .select('category', 'overAllMotivation', 'year', f.explode('laureates').alias('laureate')) # could introduce the monotonic increasing id, but 'category', 'year' should be good "column" indicator
    .withColumn('laureate_cleansed', f.struct(f.col('laureate.firstname')))
    .groupBy('category', 'overAllMotivation', 'year')
    .agg(
        f.collect_list('laureate_cleansed').alias('laureates')
    )
)

# COMMAND ----------

# DBTITLE 1,Quest: Join..
# QUEST: compute pairwise differences in population among countries

# COMMAND ----------

# Answer...

df_pairwise = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .select(f.col('country').alias('a_country'), f.col('pop').alias('a_pop'))
    .crossJoin(
        df_gapminder
        .filter(f.col('year') == 2007)
        .select(f.col('country').alias('b_country'), f.col('pop').alias('b_pop'))
    )
    .filter(f.col('a_country') != f.col('b_country'))
    .withColumn('pop_diff', f.col('a_pop') - f.col('b_pop'))
)

display(df_pairwise)
