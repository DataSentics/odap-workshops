# Databricks notebook source
# MAGIC %md
# MAGIC # Oh no my Git is not working in Databricks :(

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Enable "Files in Repos"
# MAGIC Enable 'Files in Repos' in your Databricks workspace at Settings -> Admin Console -> Workspace Settings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up a GitHub personal access token
# MAGIC 1. Create [GitHub personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
# MAGIC   1. In GitHub, follow these steps to create a personal access token that allows access to your repositories:
# MAGIC   2. In the upper-right corner of any page, click your profile photo, then click Settings.
# MAGIC   3. Click Developer settings.
# MAGIC   4. Click the Personal access tokens tab.
# MAGIC   5. Click the Generate new token button.
# MAGIC   6. Enter a token description.
# MAGIC   7. Select the repo permission, and click the Generate token button.
# MAGIC 2. In your Databricks workspace at Settings -> User Settings -> Git Integration select GitHub as a provider and use your new token here

# COMMAND ----------

![token-permissions](https://github.com/DataSentics/odap-workshops/tbo-git/Git/images/github-newtoken.png?raw=true)

# COMMAND ----------


