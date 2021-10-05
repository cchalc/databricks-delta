# Databricks notebook source
# MAGIC %run ./globals

# COMMAND ----------

# DBTITLE 1,Cleanup
# Not much to clean-up since everything is kept local to the project-specific database.
# So, to clean-up, just drop that database.
spark.sql( """
drop database if exists {} cascade
""".format( database ) )

pass

# COMMAND ----------

display( dbutils.fs.ls(project) )

# COMMAND ----------


