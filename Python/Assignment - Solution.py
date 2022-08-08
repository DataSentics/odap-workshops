# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment - Solution
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

# For how many people we have income information about?
df_droped_na = df_all.dropna(subset=["MonthlyIncome"])
df_droped_na.shape[0]

# COMMAND ----------

# Which male and female who survived has the highest income?
df_females = df_droped_na[df_droped_na["Sex"] == 'female']
df_males = df_droped_na[df_droped_na["Sex"] == 'male']
df_droped_na[(df_droped_na["MonthlyIncome"] == df_females["MonthlyIncome"].max()) | (df_droped_na["MonthlyIncome"] == df_males["MonthlyIncome"].max())]

# COMMAND ----------

# What is the average savings of people that did not survive?
df_droped_na[df_droped_na["Survived"] == 0]["Savings"].mean()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4
# MAGIC * create decorate function for time measuring

# COMMAND ----------

def log(func):
    def inner(*args, **kwargs):
        print("Accessed the function '{}' with arguments {}".format(func.__name__, args, kwargs))
        return func(*args, **kwargs)
    return inner

@log
def func(x):
    return 2 * x

print(func(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5
# MAGIC * From the first dataset remove features (PassengerId, Name, Cabin, Ticket)
# MAGIC * Train classifier of your choice (from Scikit-learn library) on the new dataset - with the decorate function measure training time
# MAGIC * Find model accurancy
# MAGIC * Visualize confusion matrix
# MAGIC * Visualize feature importance

# COMMAND ----------

df_titanic

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

# TODO Perform the train test split
x_train,x_test,y_train,y_test = train_test_split(df_titanic_features, df_titanic_label, test_size=0.25, random_state=123)
# TODO Create RandomForestClassifier from the imported module RandomForestClassifier. It is already imported for you
clf_forest = RandomForestClassifier(random_state=0)
# TODO Fit the  RandomForestClassifier
clf_forest.fit(x_train, y_train)
# TODO Make a prediction on RandomForestClassifier 
clf_forest.predict(x_test)

# COMMAND ----------

# TODO show the cunfussion matrix
plot_confusion_matrix(clf_forest, x_test, y_test)
plt.show()

# COMMAND ----------

importances = clf_forest.feature_importances_
feature_importances = pd.Series(importances, index=df_titanic_features.columns)
feature_importances.sort_values(ascending = False).head(10).plot.bar(figsize=[10,8])
