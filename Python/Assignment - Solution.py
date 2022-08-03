# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment - Solution
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

df_titanic = pd.read_csv(source_titanic)
df_titanic_income = pd.read_csv(source_titanic_income)

# COMMAND ----------

df_titanic_income

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2
# MAGIC * For the first dataset (titanic) answer questions below:
# MAGIC   1) According to Wikipedia, there was an estimated 2,224 passengers and crew onboard the Titanic when it sank. How many of them do we have information for in this dataset?
# MAGIC   2) Of the people we have data for, how many of them survived and how many did not? (Visualize the result as barchart.)
# MAGIC   3) What is the overall survival rate?
# MAGIC   4) How many passengers on the Titanic were males and how many were females in each ticket class?

# COMMAND ----------

# How many of them do we have information for in this dataset?
len(df_titanic["Name"].unique())

# COMMAND ----------

# How many of them survived and how many did not? (Visualize the result as barchart.)
df_titanic["Survived"].value_counts().plot.bar()

# COMMAND ----------

# What is the overall survival rate?
(df_titanic[df_titanic["Survived"] == 1].shape[0] / (df_titanic.shape[0] / 100))

# COMMAND ----------

# How many passengers on the Titanic were males and how many were females in each ticket class?
df_titanic[["PassengerId", "Pclass", "Sex"]].groupby(["Pclass", "Sex"]).count()

# COMMAND ----------

df_titanic_income

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3
# MAGIC * Join the two datasets on "id" and answer questions below:
# MAGIC   1) For how many people we have income information about?
# MAGIC   2) Which male and female who survived has the highest income?
# MAGIC   3) What is the average savings of people that did not survive?

# COMMAND ----------

# Join the two datasets on "PassengerId"
df_all = pd.merge(df_titanic, df_titanic_income, how="left", on="PassengerId")
df_all

# COMMAND ----------

df_droped_na = df_all.dropna(subset=["MonthlyIncome"])
df_droped_na

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4
# MAGIC * create decorate function for time measuring

# COMMAND ----------

# importing libraries
import time
import math
 
# decorator to calculate duration
# taken by any function.
def calculate_time(func):
     
    # added arguments inside the inner1,
    # if function takes any arguments,
    # can be added like this.
    def inner1(*args, **kwargs):
 
        # storing time before function execution
        begin = time.time()
         
        func(*args, **kwargs)
 
        # storing time after function execution
        end = time.time()
        print("Total time taken in : ", func.__name__, end - begin)
 
    return inner1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5
# MAGIC * From the first dataset remove features (PassengerId, Name, Cabine)
# MAGIC * Train classifier of your choice (from Scikit-learn library) on the new dataset - with the decorate function measure training time
# MAGIC * Find model accurancy
# MAGIC * Visualize confusion matrix
# MAGIC * Visualize feature importance
