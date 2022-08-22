# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebooks like an interactive tool for development
# MAGIC Notebooks should not be used as a way to share graphs and results in interactive manner. If we want to be sharing dashboards we need to use SQL dashboards. The only way for interactivity for the people that do not know how to code in notebooks is to use widgets. You can create a new dashboard from a cell. This cell can than be refreshed from the dashboard. The parameters to the dashboard can be than changed using widgets.

# COMMAND ----------

dbutils.widgets.text("name", "")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS example_dashboard;
# MAGIC CREATE TABLE IF NOT EXISTS example_dashboard(id INT, value INT, names STRING);
# MAGIC INSERT INTO example_dashboard VALUES (1, 200, "Martin"), (2, 200, "Martin"), (3, 400, "Joseph"), (4, 600, "Joseph");

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS example_dashboard_two;
# MAGIC CREATE TABLE IF NOT EXISTS example_dashboard_two(id INT, value INT, names STRING);
# MAGIC INSERT INTO example_dashboard_two VALUES (1, 1234, "Martin"), (2, 5678, "Martin"), (3, 1251, "Joseph"), (4, 5322, "Joseph");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${name}

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Dashboards
# MAGIC Another way to use dashboards is SQL dashboards. You just create a query and you are ready to run it. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event triggered jobs
# MAGIC Unfortunately currently event triggered jobs are not supported. Databricks workflows is still under development and they should come soon. But if you are concerned about triggering job on adding a file to certain cloud storage this is possible using auto-loader and cloud files. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reusing functions in databricks
# MAGIC One way is to run different notebook from current notebook using ```dbutils.notebooks.run()```. This way the functions from called notebook will become available in current notebook. But this is anti-pattern because you do no know which functions has been imported. 
# MAGIC 
# MAGIC Better way is to define .py file in which you define all your functions. Just like in a normal python project. 

# COMMAND ----------

from functions import multiplier
