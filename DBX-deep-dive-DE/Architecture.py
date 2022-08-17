# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks deeper

# COMMAND ----------

# MAGIC %md
# MAGIC ##High-level architecture
# MAGIC Azure Databricks is structured to enable secure cross-functional team collaboration while keeping a significant amount of backend services managed by Azure Databricks so you can stay focused on your data science, data analytics, and data engineering tasks.
# MAGIC 
# MAGIC Azure Databricks operates out of a control plane and a data plane.
# MAGIC 
# MAGIC * The control plane includes the backend services that Azure Databricks manages in its own Azure account. Notebook commands and many other workspace configurations are stored in the control plane and encrypted at rest.
# MAGIC * The data plane is managed by your Azure account and is where your data resides. This is also where data is processed. You can use Azure Databricks connectors so that your clusters can connect to external data sources outside of your Azure account to ingest data or for storage. You can also ingest data from external streaming data sources, such as events data, streaming data, IoT data, and more.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separated storage and compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality pipeline
# MAGIC 
# MAGIC The Bronze/Silver/Gold in the above picture are just layers in your data lake. Bronze is raw ingestion, Silver is the filtered and cleaned data, and Gold is business-level aggregates. This is just a suggestion on how to organize your data lake, with each layer having various Delta Lake tables that contain the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog
# MAGIC [Introduction](https://www.databricks.com/product/unity-catalog)
# MAGIC 
# MAGIC Unity Catalog is a unified governance solution for all data and AI assets including files, tables and machine learning models in your lakehouse on any cloud.
