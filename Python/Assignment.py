# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment
# MAGIC * This is assignment for practice Pandas, Scikit-learn and Decorators.
# MAGIC * Feel free to use all notebooks with examples and documentations.

# COMMAND ----------

# Here you can add imports
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Paths to datasets
# run
source_titanic = "./data/titanic.csv"
source_titanic_income = "./data/titanic_income_savings.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1
# MAGIC * Read datasets titanc and titanic_income as pandas dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2
# MAGIC * For the first dataset (titanic) answer questions below:
# MAGIC   1) According to Wikipedia, there was an estimated 2,224 passengers and crew onboard the Titanic when it sank. How many of them do we have information for in this dataset?
# MAGIC   2) Of the people we have data for, how many of them survived and how many did not? (Visualize the result as barchart.)
# MAGIC   3) What is the overall survival rate?
# MAGIC   4) How many passengers on the Titanic were males and how many were females in each ticket class?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3
# MAGIC * Join the two datasets on "id" and answer questions below:
# MAGIC   1) For how many people we have income information about?
# MAGIC   2) Which male and female who survived has the highest income?
# MAGIC   3) What is the average savings of people that did not survive?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4
# MAGIC * From the first dataset remove features (PassengerId, Name, Cabine)
# MAGIC * Train classifier of your choice (from Scikit-learn library) on the new dataset
# MAGIC * Find model accurancy
# MAGIC * Visualize confusion matrix
# MAGIC * Visualize feature importance
