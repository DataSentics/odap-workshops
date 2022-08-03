# Databricks notebook source
# MAGIC %md
# MAGIC # Scikit-learn
# MAGIC * [Documentation](https://scikit-learn.org/stable/)

# COMMAND ----------

import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics import plot_confusion_matrix
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score

# COMMAND ----------

df_iris = pd.read_csv("./data/iris.csv")
df_iris

# COMMAND ----------

# DBTITLE 1,Convert target variable to numbers
le = preprocessing.LabelEncoder()
le.fit(df_iris['Name'])
df_iris['Name']=le.transform(df_iris['Name'])
df_iris

# COMMAND ----------

# DBTITLE 1,Remove target variable from training data
df_iris_label = df_iris["Name"]
df_iris_data = df_iris.drop(columns=["Name"])

# COMMAND ----------

# DBTITLE 1,Split dataset to train and test data
# test_size = fraction of data which is used for testing
x_train,x_test,y_train,y_test = train_test_split(df_iris_data, df_iris_label, test_size=0.3, random_state=123)

# COMMAND ----------

# DBTITLE 1,Train RandomForesTree
forest = RandomForestClassifier(random_state=0)
forest.fit(x_train, y_train)

# COMMAND ----------

# DBTITLE 1,Prediction on test data
predicted = clf.predict(x_test)
predicted

# COMMAND ----------

# DBTITLE 1,Model accurancy
accuracy_score(y_test, predicted, normalize=True)

# COMMAND ----------

# DBTITLE 1,Visualize confusion matrix
plot_confusion_matrix(forest, x_test, y_test)
plt.show()

# COMMAND ----------

# DBTITLE 1,Visualize feature importance
importances = forest.feature_importances_
forest_importances = pd.Series(importances, index=df_iris_data.columns)
forest_importances.plot.bar()
