# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment
# MAGIC * This is assignment for practice Pandas, Scikit-learn and Decorators.
# MAGIC * Feel free to use all notebooks with examples and documentations.

# COMMAND ----------

# Here you can add imports
import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics import plot_confusion_matrix
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.metrics import mean_squared_error

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
# MAGIC * define simple function with some parameters which will by decorated by log

# COMMAND ----------

def log(func):
    def inner(*args, **kwargs):
        print("Accessed the function '{}' with arguments {}".format(func.__name__, args, kwargs))
        return func(*args, **kwargs)
    return inner



# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5
# MAGIC * From the first dataset remove features (PassengerId, Name, Cabine)
# MAGIC * Train classifier of your choice (from Scikit-learn library) on the new dataset - with the decorate function measure training time
# MAGIC * Find model accurancy
# MAGIC * Visualize confusion matrix
# MAGIC * Visualize feature importance

# COMMAND ----------

# First we need to drop null values because..
df_titanic = df_titanic.dropna()
# Then we need to encode the labels because ...
df_titanic_label = df_titanic["Survived"]
label_encoder = preprocessing.LabelEncoder()
label_encoder.fit(df_titanic['Survived'])
df_titanic['Survived']=label_encoder.transform(df_titanic['Survived'])
# Than we need to drop .... because 
df_titanic_features = df_titanic.drop(columns=["PassengerId", "Name", "Cabin", "Survived", "Ticket"])
df_titanic_features = pd.get_dummies(data=df_titanic_features, drop_first=False)

# TODO Perform the train test split on df_titanc_features and df_titanc_label
x_train,x_test,y_train,y_test = 
# TODO Create RandomForestClassifier from the imported module RandomForestClassifier. It is already imported for you
clf_forest = 
# TODO Fit the  RandomForestClassifier

# TODO Make a prediction on RandomForestClassifier

