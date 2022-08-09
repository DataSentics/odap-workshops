# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks introduction
# MAGIC 
# MAGIC * [Documentation](https://docs.databricks.com/index.html)
# MAGIC 
# MAGIC Databricks is a Data Lakehous platform. Data Lakehouse unifies the best of data warehouses and data lakes in one simple platform to handle all your data, analytics and AI use cases. It’s built on an open and reliable data foundation that efficiently handles all data types and applies one common security and governance approach across all of your data and cloud platforms.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL
# MAGIC * [Ducumentation](https://www.databricks.com/product/databricks-sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Machine Learning
# MAGIC * [Documentation](https://www.databricks.com/product/machine-learning)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Science and Data Engineering
# MAGIC * ### Workspace
# MAGIC 
# MAGIC     A Databricks workspace is an environment for accessing all of your Databricks assets. The workspace organizes objects (notebooks, libraries, and experiments) into folders, and provides access to data and computational resources such as clusters and jobs.
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
# MAGIC   Contains all tables and file system.
# MAGIC * ### Compute
# MAGIC * ### Workflows
# MAGIC * ### Partner Connect
# MAGIC * ### Help
# MAGIC * ### Settings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook - utils
# MAGIC * [Documentation](https://docs.databricks.com/dev-tools/databricks-utils.html)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook - cells

# COMMAND ----------

# DBTITLE 1,Run another notebook
# MAGIC %run ./test-notebook

# COMMAND ----------

# DBTITLE 1,Install package
# MAGIC %pip install matplotlib

# COMMAND ----------

# DBTITLE 1,SQL cell
# MAGIC %sql

# COMMAND ----------

# DBTITLE 1,Python cell
print()

# COMMAND ----------

# DBTITLE 1,Table output
df = spark.read.option("header", "true").csv('file:/dbfs/FileStore/python-workshop/tips.csv')
display(df)
