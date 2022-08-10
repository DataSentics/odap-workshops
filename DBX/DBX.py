# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks introduction
# MAGIC 
# MAGIC * [Documentation](https://docs.databricks.com/index.html)
# MAGIC 
# MAGIC #### Databricks is an ultimate platform to handle all your data, analytics and AI use cases. It offers effective cooperation between Data Engineers and Data Scientists.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL
# MAGIC * [Ducumentation](https://www.databricks.com/product/databricks-sql)
# MAGIC 
# MAGIC SQL workspace provides a native SQL interface and query editor, integrates well with existing BI tools, supports the querying of data in Delta Lake using SQL queries, and offers the ability to create and share visualizations. With SQL Analytics, administrators can granularly access and gain insights into how data in being accessed within the Lakehouse through usage and phases of the query's execution process. With its tightly coupled Integration with Delta Lake, SQL Analytics offers reliable governance of data for audit and compliance needs. Users will have the ability to easily create visualizations in the form of dashboards, and will not have to worry about creating and managing complex Apache Spark based compute clusters with SQL serverless endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Machine Learning
# MAGIC * [Documentation](https://www.databricks.com/product/machine-learning)
# MAGIC 
# MAGIC Machine Learning workspace is very similar to Data Science and Data Engineering workspace, but in addition, this Machine Learning workspace offers added components for Experiments, Feature Stores, and ML Models. This workspace supports an end-to-end machine learning environments including robust components for managing feature development, model training, model serving, and experiment tracking. Models can be trained manually or through AutoML. They can be tracked with MLFlow tracking and support the creation of feature tables for model training and inferencing. Models can then be stored, shared and served in the Model Registry.
# MAGIC 
# MAGIC Experiment = The main unit of organization for tracking machine learning model development. Experiments organize, display, and control access to individual logged runs of model training code.
# MAGIC 
# MAGIC Feature Store = a centralized repository of features. Databricks Feature Store enables feature sharing and discovery across your organization and also ensures that the same feature computation code is used for model training and inference.
# MAGIC 
# MAGIC Model Registry = provides a Model Registry for managing the lifecycle of MLFlow Models. It provides model versioning, stage promotion details, model lineage, and supports email notifications.
# MAGIC 
# MAGIC Models = a trained machine learning or deep learning model that has been registered in Model Registry.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Science & Data Engineering
# MAGIC 
# MAGIC Data Science & Data Engineering workspace is the most common workspace used by Data Engineering and Data Science professionals. Within this workspace, you will be able to create notebooks for writing code in either Python, Scala, SQL, or R languages. Notebooks can be shared, secured with visibility and access control policies, organized in hierarchical folder structures, and attached to a variety of high-powered compute clusters. These compute clusters can be attached at either the workspace or notebook level. It is within these notebooks where you will also be able to render your code execution results in either tabular, chart, or graph format.
# MAGIC 
# MAGIC * ### Workspace
# MAGIC 
# MAGIC     Your folder with your notebooks, libraries and MLFlow experiments.
# MAGIC 
# MAGIC * ### Repos
# MAGIC     * Git functionality such as cloning a remote repo, managing branches, pushing and pulling changes, and visually comparing differences upon commit.
# MAGIC     * Provides an API that you can integrate with your CI/CD pipeline.
# MAGIC     * Provides security features such as allow lists to control access to Git repositories and detection of clear text secrets in source code.
# MAGIC     * Databricks supports these Git providers:
# MAGIC     
# MAGIC       - GitHub
# MAGIC       - Bitbucket Cloud
# MAGIC       - GitLab
# MAGIC       - Azure DevOps (not available in Azure China regions)
# MAGIC       - AWS CodeCommit
# MAGIC       - GitHub AE
# MAGIC 
# MAGIC * ### Recents
# MAGIC   List of your recently viewd files.
# MAGIC   
# MAGIC * ### Search
# MAGIC   Allowes to search throught tables, notebooks, files and folders.
# MAGIC   
# MAGIC * ### Data
# MAGIC   Access to all database tables and file system.
# MAGIC   
# MAGIC   #### Databricks File System (DBFS)
# MAGIC 
# MAGIC     A filesystem abstraction layer over a blob store. It contains directories, which can contain files (data files, libraries, and images), and other directories. DBFS is automatically populated with some datasets that you can use to learn Azure Databricks.
# MAGIC 
# MAGIC 
# MAGIC * ### Compute
# MAGIC * ### Workflows
# MAGIC * ### Partner Connect
# MAGIC * ### Help
# MAGIC * ### Settings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks
# MAGIC 
# MAGIC * Revision history
# MAGIC * Markdown + 4 languages (default python):

# COMMAND ----------

# for loop in Python
my_list = [2, 5, "apple", 78]
for i in my_list:
    print(i)

# COMMAND ----------

# MAGIC %scala
# MAGIC // for loop in Scala
# MAGIC var myArray = Array(2, 5, "apple", 78)
# MAGIC for (i <- myArray){
# MAGIC     println(i)
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC x <- c(2, 5, "apple", 78)
# MAGIC for (val in x) {
# MAGIC  print(val)
# MAGIC }

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iris_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run another notebook

# COMMAND ----------

# MAGIC %run ./test-notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install package

# COMMAND ----------

# MAGIC %pip install matplotlib

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Utils
# MAGIC * [Documentation](https://docs.databricks.com/dev-tools/databricks-utils.html)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/python-workshop")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

# DBTITLE 1,Dropbox
dbutils.widgets.dropdown("state", "CZ", ["CZ", "SK", "PL", "D"])

# COMMAND ----------

state = dbutils.widgets.get("state")
print(f"I am from {state}.")

# COMMAND ----------

dbutils.widgets.remove("state")

# COMMAND ----------

# DBTITLE 1,Text
dbutils.widgets.text("name", "")

# COMMAND ----------

name = dbutils.widgets.get("name")
print(f"Hello {name}!")

# COMMAND ----------

# DBTITLE 1,Combobox
dbutils.widgets.combobox("fruit", "apple", ["apple", "orange", "banana", "strawberry"])

# COMMAND ----------

fruit = dbutils.widgets.get("fruit")
print(f"I like {fruit}.")

# COMMAND ----------

# DBTITLE 1,Multiselect
dbutils.widgets.multiselect("genre", "comedy", ["action", "adventure", "comedy", "crime", "fantasy", "historical", "horror", "romance", "satira"])

# COMMAND ----------

# Attention: multiselect output is a string with elements separated by commas. NOT list as you probably would expect.
genre = dbutils.widgets.get("genre")
print(f"My favorite movie is {genre}.")

# COMMAND ----------

# Remove all widgets.
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utils + Widgets example

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = username.split("@")[0]
username = username.replace('.', '_')
database_name = username + "_db"

dbutils.widgets.dropdown("table", "iris_table", [database[0] for database in spark.catalog.listTables(database_name)])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ${table}
# MAGIC LIMIT 100

# COMMAND ----------

# TODO create a simple notebook and run it in this cell

# COMMAND ----------

# TODO create new dropbox widget with the types of iris {"Iris-setosa", "Iris-versicolor", "Iris-virginica"} from table iris_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO write SQL query which will select all rows with Name = {value in widget} from table iris_table

# COMMAND ----------

# TODO remove all widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data explorer
# MAGIC 
# MAGIC * ### Databricks File System (DBFS)
# MAGIC A filesystem abstraction layer over a blob store. It contains directories, which can contain files (data files, libraries, and images), and other directories.
# MAGIC 
# MAGIC * ### Database
# MAGIC A collection of information that is organized so that it can be easily accessed, managed, and updated.
# MAGIC 
# MAGIC * #### Table
# MAGIC A representation of structured data. You query tables with Apache Spark SQL and Apache Spark APIs.
# MAGIC 
# MAGIC * ### Metastore
# MAGIC The component that stores all the structure information of the various tables and partitions in the data warehouse including column and column type information, the serializers and deserializers necessary to read and write data, and the corresponding files where the data is stored.
# MAGIC 
# MAGIC * ### Delta Table

# COMMAND ----------

# DBTITLE 1,Delta Table
df = spark.read.option("header", "true").csv('file:/dbfs/FileStore/python-workshop/tips.csv')

print("Table from csv:")
display(df)

(
df.write
  .mode("overwrite")
  .partitionBy("day")
  .format("delta")
  .save("/FileStore/dbx-workshop/tips")
)

df_read =(spark
      .read
      .format("delta")
      .load("/FileStore/dbx-workshop/tips")
    )

print("Table from delta:")
display(df_read)

table_name = "tips_from_delta"
delta_path = "/FileStore/dbx-workshop/tips"
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{delta_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now we can query the delta table as SQL table
# MAGIC SELECT *
# MAGIC FROM tips_from_delta;

# COMMAND ----------

# TODO: Just run this cell. And fill in your first name.
dbutils.widgets.text("your_name", "")

# COMMAND ----------

# TODO:
# 1) read csv file file:/dbfs/FileStore/python-workshop/iris.csv as spark dataframe
# 2) write dataframe as delta table partitioned by 'Name' - as path use destination_iris
# 3) find the delta table in Data explorer -> hive_metastore -> default -> {your table}
# 4) find the delta table in DBFS by path /FileStore/dbx-workshop/iris_{your name}
source_iris = 'file:/dbfs/FileStore/python-workshop/iris.csv'
your_name = dbutils.widgets.get("your_name")
destination_iris = f'/FileStore/dbx-workshop/iris_{your_name}'

# COMMAND ----------

# TODO: Just run this cell.
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflows
# MAGIC 
# MAGIC Frameworks to develop and run data processing pipelines:
# MAGIC 
# MAGIC ### Workflows with jobs:
# MAGIC A non-interactive mechanism for running a notebook or library either immediately or on a scheduled basis.
# MAGIC 
# MAGIC ### Delta Live Tables:
# MAGIC A framework for building reliable, maintainable, and testable data processing pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clusters
# MAGIC 
# MAGIC ### 
