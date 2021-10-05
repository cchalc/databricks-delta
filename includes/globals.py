# Databricks notebook source
# Global variables used by both setup and cleanup

# Full username, e.g. "aaron.binns@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use in a database name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project
project = "/user/{}/golden_demos/delta".format(username)

# Name of database for this project
database = user + "_demo_delta"

# COMMAND ----------


