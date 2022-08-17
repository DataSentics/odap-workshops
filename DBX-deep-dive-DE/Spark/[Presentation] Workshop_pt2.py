# Databricks notebook source
# MAGIC %md
# MAGIC # Please clone this into your own folder=

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Workshop
# MAGIC ## PART 2 - (py)spark SQL module
# MAGIC 
# MAGIC - Basic operations - repetition
# MAGIC - Projections: expressions, UDF..
# MAGIC - Aggregations
# MAGIC - Window functions
# MAGIC - Partitions: Coalesce, repartition, shuffle, stage..
# MAGIC - Complex data types (array, struct, map)
# MAGIC - Join, broadcast join

# COMMAND ----------

# MAGIC %md
# MAGIC - Note 1: Reference is your friend! [https://spark.apache.org/docs/latest/api/python/pyspark.sql.html]
# MAGIC - SQL ref [https://spark.apache.org/docs/2.3.1/api/sql/index.html#repeat]
# MAGIC - Note 2: Naming and PEP-like conventions are your friends! [https://www.python.org/dev/peps/pep-0008/]

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Imports

# COMMAND ----------

# Import only what you need
#from pyspark.sql.functions import col, lit, collect_list #...

# Or (imho better), whole modules
import pyspark.sql.functions as f
import pyspark.sql.types as T


# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Read data

# COMMAND ----------

"""spark.sql(f"DROP DATABASE IF EXISTS demo CASCADE")

spark.sql(f"CREATE DATABASE demo")

spark.sql(f"USE DATABASE demo")"""

# COMMAND ----------

"""schema = (T.StructType()
          .add("country",T.StringType(),True)
          .add("continent",T.StringType(),True)
          .add("lifeExp",T.DoubleType(),True)
          .add("pop",T.IntegerType(),True)
          .add("gdpPercap",T.DoubleType(),True)
          .add("year",T.IntegerType(),True)
         )"""

schema = (T.StructType()
          .add("laureates", T.StringType(),True)
          .add("overallMotivation",T.StringType(),True)
          .add("category",T.StringType(),True)
          .add("year",T.IntegerType(),True)
         )

json_schema = (T.ArrayType(
                T.StructType([
                  T.StructField('firstname', T.StringType(), nullable=False),
                  T.StructField('id', T.IntegerType(), nullable=False),
                  T.StructField('motivation', T.StringType(), nullable=False),
                  T.StructField('share', T.IntegerType(), nullable=False),
                  T.StructField('surname', T.StringType(), nullable=False)])))

df = spark.read.schema(schema).option("header", "true").option("quote", "'").option("sep", '\t').csv('file:/dbfs/FileStore/dbx-workshop/nobel_prize.csv')

df = df.withColumn("laureates", f.from_json(f.col('laureates'), json_schema))
print("Table from csv:")
display(df)

(
  df.write
    .mode("overwrite")
    .partitionBy("year")
    .format("delta")
    .option("path", "/FileStore/dbx-workshop/nobel_prize")
    .option("overwriteSchema", "True")
    .saveAsTable("demo.nobel_prize")
)

# COMMAND ----------

df_gapminder = (spark.read.table('demo.gapminder_basic_stats'))

# COMMAND ----------

df_gapminder = (
    spark
    .read
    .table('demo.gapminder_basic_stats')
    #.parquet(..)
)

# COMMAND ----------

df_gapminder.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Basic operations (recap?)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect

# COMMAND ----------

display(df_gapminder)  # databricks util

# COMMAND ----------

df_gapminder.show() # pyspark method

# COMMAND ----------

df_gapminder.count()

# COMMAND ----------

df_gapminder.describe().show()

# COMMAND ----------

df_gapminder.registerTempTable('gapminder')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gapminder

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Projection operators

# COMMAND ----------

# DBTITLE 1,Select & rename
# Select and rename
df_pop_per_country_year = df_gapminder.select('country', 'year', f.col('pop').alias('population'))

display(df_pop_per_country_year)

# COMMAND ----------

f.col('pop').alias('population')

# COMMAND ----------

# DBTITLE 1,withColumnRenamed
# Withcolumnrenamed

df_pop_per_country_year2 = (
    df_gapminder
    .select('country', 'year', 'pop')
    .withColumnRenamed('pop', 'population')  # existing name, new name
)

display(df_pop_per_country_year2)

# COMMAND ----------

# DBTITLE 1,withColumn
# Measure population in millions

df_popmil_per_country_year = (
    df_gapminder
    .select('country', 'year', 'pop')
  #  .withColumn('population_mil1', 'pop / 1000000')  # on purpose
    .withColumn('population_mil2', f.col('pop') / 1000000)
    .withColumn('population_mil3', f.col('pop') / f.lit(1000000))  # new name, expression using existing columns!
)

display(df_popmil_per_country_year)

# COMMAND ----------

# DBTITLE 1,drop
display(
    df_popmil_per_country_year
    .drop('pop')
)

# COMMAND ----------

# DBTITLE 1,selectExpr
# Select expression
df_popmil_per_country_year2 = (
    df_gapminder
    .selectExpr('country', 'year', 'pop / 1000000 as population_mil')
)

display(df_popmil_per_country_year2)

# COMMAND ----------

# DBTITLE 1,withColumn expression
# Measure population in millions

expr_pop_mil = f.expr('pop / 1000000')

df_popmil_per_country_year3 = (
    df_gapminder
    .select('country', 'year', 'pop')
    #.withColumn('population_mil', f.col('pop') / 1000000)  # previous solution
    #.withColumn('population_mil', 'pop / 1000000')  # on purpose
    #.withColumn('population_mil', f.expr('pop / 1000000'))
    .withColumn('population_mil', expr_pop_mil)
)

display(df_popmil_per_country_year3)

# COMMAND ----------

# DBTITLE 1,Conditions
# Assume that GDP before 1980 was calculated differently, needs to be corrected by a certain factor
df_gdp_correction = (
    df_gapminder
    .withColumn(
        'gdp_corrected',
        f.when(f.col('year') < 1980, f.col('gdpPercap') * 1.04)  # More on conditions later
        .when(f.col('year') < 1990, f.col('gdpPercap') * 2.04)  # More on conditions later
        .otherwise(f.col('gdpPercap'))
    )
)

display(df_gdp_correction)

# COMMAND ----------

# DBTITLE 1,Function on one column
# log of population
df_log_pop = (
    df_gapminder
    .withColumn('population_log', f.log('pop'))
)

display(df_log_pop)

# COMMAND ----------

# DBTITLE 1,Function of two column values
# Log of gdp with base 2

# Check the reference. Not always helpful, need to try :)

df_log_pop2 = (
    df_gapminder
    #.withColumn('gdp_log', f.log(2.0, f.col('gdpPercap')))
    .withColumn('base', f.lit(2))
    #.withColumn('gdp_log', f.log(f.col('base'), f.col('gdpPercap')))
    #.withColumn('gdp_log', f.log('base', f.col('gdpPercap')))
    .withColumn('gdp_log', f.expr('log(base, gdpPercap)'))
)

display(df_log_pop2)

# COMMAND ----------

# DBTITLE 1,Simple UDF
# UDF
import math

@udf(T.DoubleType())
def log_a_base_b(number, base):
    result = math.log(number, base) 
    return result
  
  
df_log_pop3 = (
    df_gapminder
    .withColumn('base', f.lit(2))
    .withColumn('gdp_log', log_a_base_b(f.col('gdpPercap'), f.col('base')))
)

display(df_log_pop3)

# COMMAND ----------

# Pandas UDF - not so simple to do a function of two pandas series

# pdf_gapminder = df_log_pop3.toPandas()
# math.log(pdf_gapminder.gdpPercap, pdf_gapminder.base)

# import math

# @f.pandas_udf("double", f.PandasUDFType.SCALAR)
# def log_a_base_b_pd(number, base):
#     result = math.log(number, base) 
#     return result
  
  
# df_log_pop4 = (
#     df_gapminder
#     .withColumn('base', f.lit(2))
#     .withColumn('gdp_log', log_a_base_b_pd(f.col('gdpPercap'), f.col('base')))
# )

# display(df_log_pop4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter

# COMMAND ----------

# DBTITLE 0,[!] Exercise
# GDP per capita in CZ in 1990-1999
df_gdp_cz_90s = (
    df_gapminder
    .where(f.col('country') == 'Czech Republic')
    .filter(
        #f.col('year') >= 1990 & f.col('year') <= 1999
        (f.col('year') >= 1990) & (f.col('year') <= 1999)
    )
    .select('year', 'gdpPercap')
)

display(df_gdp_cz_90s)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort

# COMMAND ----------

# Largest countries in year 2007
df_largest_countries_2007 = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .orderBy('pop', ascending = False)
    #.orderBy(f.desc('pop'))
    .select('country', 'pop')
)

display(df_largest_countries_2007)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Single DF transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregation
# MAGIC Multiple rows -> one row
# MAGIC 
# MAGIC [https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData]

# COMMAND ----------

# Stats per continent: 

# Number of countries per continent (in 2007)

df_countries_per_continent = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .groupBy('continent')
    .count()  # GroupedData.count
    .orderBy(f.desc('count'))  # how do we know the name 'count'? Caution: databricks certification :-)
)

display(df_countries_per_continent)

# total population per continent in 2007

# COMMAND ----------

# More explicit way

df_countries_per_continent2 = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .groupBy('continent')
    .agg(
        f.count('*').alias('ncountries')  # functions.count
    )
    .orderBy(f.desc('ncountries'))
)

display(df_countries_per_continent2)

# COMMAND ----------

df_gapminder.rdd.getNumPartitions()

# COMMAND ----------

# Make sure we count all the countries, year 2007 may not be complete

df_countries_per_continent3 = (
    df_gapminder
    .groupBy('continent')
    .agg(
        f.countDistinct('country').alias('ncountries')  # functions.countDistinct
    )
    .orderBy(f.desc('ncountries'))
)

display(df_countries_per_continent3)

# COMMAND ----------

df_stats_per_continent_2007 = (
    spark.read.table('demo.gapminder_basic_stats')
    .filter(f.col('year') == 2007)
    .groupBy('continent')
    .agg(
        f.count('country').alias('ncountries'),
        f.min('pop').alias('population_min'),
        f.avg('pop').alias('population_avg'),
        f.max('pop').alias('population_max'),
        f.sum('pop').alias('population_sum')
    )
    .orderBy(f.desc('population_sum'))
    .withColumn('population_avg_wc', f.col('population_sum') / f.col('ncountries'))
)

display(df_stats_per_continent_2007)

# COMMAND ----------

# Whole lot of stats per continent in 2007

df_stats_per_continent_2007 = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .groupBy('continent')
    .agg(
        f.count('country').alias('ncountries'),
        f.min('pop').alias('population_min'),
        f.avg('pop').alias('population_avg'),
        f.max('pop').alias('population_max'),
        f.sum('pop').alias('population_sum')
    )
    .orderBy(f.desc('population_sum'))
    .withColumn('population_avg_wc', f.col('population_sum') / f.col('ncountries'))
)

display(df_stats_per_continent_2007)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window functions
# MAGIC Multiple rows -> multiple rows, based on multiple input rows
# MAGIC 
# MAGIC [https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Window]

# COMMAND ----------

# Window
from pyspark.sql import Window

# Rank countries by population in 2007

df_pop_rank = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .withColumn('population_rank', f.rank().over(Window.orderBy(f.desc('pop'))))
    .orderBy('population_rank')
)

display(df_pop_rank)

# COMMAND ----------

# Ranking within each year
df_pop_year_rank = (
    df_gapminder
    .withColumn('population_rank', f.rank().over(Window.partitionBy('year').orderBy(f.desc('pop'))))
    .orderBy('population_rank', f.desc('year'))
)

display(df_pop_year_rank)
# Indonesia, Japan and Brazil switch places

# COMMAND ----------

# Historical max per country and diff in individual years
df_pop_max = (
    df_gapminder
    .withColumn('population_max', f.max('pop').over(Window.partitionBy('country')))
    .withColumn('pop_below_max', f.col('population_max') - f.col('pop'))
    .orderBy('country', f.desc('year'))
)

display(df_pop_max)

# Can be replaced by groupBy, agg, join

# COMMAND ----------

# rangeBetween
# rowsBetween

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Partitions, stages..

# COMMAND ----------

# MAGIC %md
# MAGIC Each RDD is distributed over the cluster in a certain number of partitions, partitioned in a certain way

# COMMAND ----------

df_gapminder.rdd.getNumPartitions()

# Not a distributed dataset

# COMMAND ----------

# Change number of partitions

df_gapminder_rep = (
    df_gapminder
    .repartition(64)  # round robin
)

df_gapminder_rep.rdd.getNumPartitions()

# COMMAND ----------

# Partition by column

df_gapminder_cont = (
    df_gapminder
    .repartition('continent')
    #.repartition(64, 'continent')
)

df_gapminder_cont.rdd.getNumPartitions()

# Default number of 200 partitions. There is a config value for it.
# What is it good for? Aggregations, joins.. Spark checks whether data are partitioned in the way it needs for a given operation

# COMMAND ----------

# MAGIC %md
# MAGIC - Stages are separated by shuffles, exchanges of data among workers over the cluster
# MAGIC - Shuffles are costly: network transfer and I/O (disk)
# MAGIC - Wide transformations induce shuffle
# MAGIC - May be prevented by writing the query better, storing the data better..
# MAGIC - Probably not prevented at all costs, some costs may be higher..

# COMMAND ----------

# This query (aggregation) gets executed in two stages

display(df_stats_per_continent_2007)

# COMMAND ----------

# Partitions after aggregation

df_stats_per_continent_2007.rdd.getNumPartitions()

# groupBy('continent')

# COMMAND ----------

# Partitions after window function with partitionBy

df_pop_max.rdd.getNumPartitions()

# Window.partitionBy('country') causes shuffle into a default number of 200 partitions

# COMMAND ----------

# Decrease number of partitions

df_pop_max_rep = (
    df_pop_max
    .coalesce(64)  # does not cause shuffle, does not create a new stage
    #.repartition(64)  # does a shuffle
)

df_pop_max_rep.rdd.getNumPartitions()

# COMMAND ----------

df_pop_max_rep.show()

# COMMAND ----------

# Beware of collapse of partitioning!

df_count = (
    df_gapminder
    .repartition(200)
    .groupBy()
    .agg(
        f.count('*').alias('count')
    )
)

df_count.rdd.getNumPartitions()

# COMMAND ----------

# Beware of window functions without partitioning!

df_pop_rank_rep = (
    df_gapminder
    .filter(f.col('year') == 2007)
    .repartition(200)
    .withColumn('population_rank', f.rank().over(Window.orderBy(f.desc('pop'))))
    .orderBy('population_rank')
)

df_pop_rank_rep.rdd.getNumPartitions()

# Count over all..

# COMMAND ----------

# MAGIC %md
# MAGIC UDFs cause spark to "forget" partitioning!

# COMMAND ----------

# Saving partitioned data?
# Parquet etc partitions useful for filtering

# Saving bucketed data into metastore?
# Useful for joins, aggregations..

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Operations with complex structures (Arrays, Structs, ...)

# COMMAND ----------

# Load `Nobel` data
df_nobel = spark.table('demo.nobel_prize')

display(df_nobel)

# COMMAND ----------

# DBTITLE 1,Explode an array
# Explode
df_nobel_exploded = (
    df_nobel
    .withColumn("laureates", f.col("laureates").stripMargin)
    .select('category', 'overAllMotivation', 'year', f.explode('laureates').alias('laureate'))
)

display(df_nobel_exploded)

# Expensive operation
# Make sure you have enough smaller partitions rather than few bigger ones, size per partition may grow a lot

# COMMAND ----------

# DBTITLE 1,Collect an array
df_nobel_unexploded = (
    df_nobel_exploded
    .groupBy('category', 'year')  # what is the id here?
    .agg(
        f.collect_set('laureate').alias('laureates'),  # order not guaranteed!
        f.first('overAllMotivation').alias('overAllMotivation')  # or can be inside grouping
    )
)

display(df_nobel_unexploded)

# COMMAND ----------

# DBTITLE 1,Accessing a struct field
# Select from struct
df_nobel_laureates_id = (
    df_nobel_exploded
    .select('laureate.firstname') # notice the dot
)

display(df_nobel_laureates_id)

# COMMAND ----------

# DBTITLE 1,Extracting all struct fields
# Select all fields from struct
df_nobel_laureates_details = (
    df_nobel_exploded
    .select('category', 'overAllMotivation', 'year', 'laureate.*') # notice the asterisk
)

display(df_nobel_laureates_details)

# COMMAND ----------

# DBTITLE 1,UDF on array
# udf (?) / other struct operations (?) / higher-order functions (?)

# Definition of UDF
@udf(T.StringType())
def get_laureates_names(x_array): 
  """ 
  Input: array/list, e.g. [{"firstname": "Max"}, {"firstname": "Liz"}]
  Output: string of concated firstnames, e.g. "Max Liz"
  """
  concated_names = ' '.join([x['firstname'] for x in x_array]) # Syntax: `"&".join(["ahoj", "svete"]` will create a string: "ahoj&svete" => "&" is the separator;
  return concated_names


# Apply the udf
df_nobel_with_concated_names = (
    df_nobel
    .withColumn('laureates_names', get_laureates_names('laureates'))
)

display(df_nobel_with_concated_names)

# As one can see: `x_array` behaved just like a list; `x` behaved just like a dictionary in Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Joins

# COMMAND ----------

"""
Read data
"""

# "Fact" table in an ugly (untidy) shape
df_income = spark.read.table('demo.gapminder_income')

# "Dimension" geography table
df_geo = spark.read.table('demo.gapminder_dim_geo')

# COMMAND ----------

# MAGIC %md
# MAGIC ### First some more (advanced?) reshaping

# COMMAND ----------

display(df_income)

# COMMAND ----------

df_income.columns

# COMMAND ----------

# Prepare a list of columns, colnames as strings are sufficient
yearcols = [colname for colname in df_income.columns if colname not in 'country']
yearcols

# COMMAND ----------

# Pack the columns into array

df_income_tidy = (
  df_income
  .withColumn('incomes', f.array(*yearcols))  # f.array wants f.array('abc', 'def', 'ghi')
  .select('country', 'incomes')
)

display(df_income_tidy)
# Tidier, but looses info about year..
# Maybe a map could be a solution?

# COMMAND ----------

# Prepare a list of column expressions in the form: key1, value1, key2, value2,..
# Maybe there is a more elegant solution? :)
yearmapcols = [[f.lit(colname), f.col(colname)] for colname in df_income.columns if colname not in 'country']
flat_list = [item for sublist in yearmapcols for item in sublist]  # flatten list of lists
yearmapcols
#flat_list

# COMMAND ----------

## Pack the columns into map

df_income_tidier = (
  df_income
  .withColumn('incomes', f.create_map(flat_list))
  .select('country', 'incomes')
)

display(df_income_tidier)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now some joins finally!

# COMMAND ----------

display(df_geo)

# COMMAND ----------

# Join expression

df_income_geo = (
    df_income_tidier
    .join(df_geo, df_income_tidier.country == df_geo.name, 'left')
)

# display(df_income_geo)
df_income_geo.show()

# COMMAND ----------

# Join expression with alias

df_income_geo2 = (
    df_income_tidier.alias('a')
    .join(df_geo.alias('b'), f.expr('a.country == b.name'), 'left')
)

display(df_income_geo2)

# COMMAND ----------

# Join using list of join columns

df_income_geo3 = (
    df_income_tidier
    .join(
        df_geo.withColumnRenamed('name', 'country'), 
        ['country'], 
        'left'
    )
)

display(df_income_geo3)

# Does not duplicate join column

# COMMAND ----------

# Broadcast join

df_income_geo4 = (
    df_income_tidier
    .join(f.broadcast(df_geo.withColumnRenamed('name', 'country')), ['country'], 'left')
)

display(df_income_geo4)


# COMMAND ----------

# MAGIC %md
# MAGIC There is a whole zoo of joins
# MAGIC 
# MAGIC inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti
# MAGIC 
# MAGIC Google and experimentation are your friends :)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write

# COMMAND ----------

(
    df_gapminder
    .repartition('year')
    .write
    .partitionBy('year')
    .bucketBy(64, 'continent')
    .option("path", "s3://...")
    .saveAsTable('demo.gapminder_pb')
)

# COMMAND ----------

df_gap = spark.read.table('demo.gapminder_pb')

df_gap.collect()

# COMMAND ----------

df_gap.groupBy('continent').count().collect()
