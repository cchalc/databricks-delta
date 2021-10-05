# Databricks notebook source
# MAGIC %run ./globals

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# COMMAND ----------

# DBTITLE 1,One-time copy of parquet data into private/personal directory.
# I like to make sure that any data I need for a demo is copied into a directory under my control.
# In fact, it turns out that the directory where this demo notebook originally expected the data to live...well, it no longer exists.
# I mean, the dbfs:/delta_webinar_inventory directory is missing :(
# Fortunately, the data is in a global (shared) table in the default database, so let's grab it from there.

master_csv_path = "/databricks-datasets/online_retail/data-001/data.csv"
inventory_path  = project + master_csv_path

# The dbuilts.fs.ls() function will throw an exception if the file/path doesn't exist, so we'll use that to check.
# If we get an excption, then the file doesn't exist (yet) in our private/personal directory and so we'll copy it there.
try:
  dbutils.fs.ls(inventory_path)
  print( "Inventory CSV file exists:", inventory_path)
except:
  # If we get an exception, then the file does not exist and we need to copy it into our private directory/folder.
  # print( "Copying master CSV file to:", inventory_path )
  dbutils.fs.cp( master_csv_path, inventory_path )

# Pass just to suppress the useless output
pass

# COMMAND ----------

# DBTITLE 1,Setup demo-specific database
spark.sql( """
drop database if exists {} cascade
""".format( database ) )

spark.sql( """
create database if not exists {}
location '{}/database/'
""".format( database, project ) )

spark.sql( """
use {}
""".format( database ) )

# Pass just to suppress the useless output
pass

# COMMAND ----------

# dbutils.fs.rm("dbfs:/user/don@databricks.com/golden_demos/delta/", True)
# %fs rm "/dbfs/user/don@databricks.com/golden_demos/delta/database/"

# COMMAND ----------

# DBTITLE 1,Copy raw data from csv file, compute item-level stats and save as "master" table for demo.
# The CSV file is basically transaction data, meaning that there will be many many rows for a single product (StockCode).
# We want to convert this transaction data into an inventory file, meaning that we have one row per product (StockCode).
# To do this, we drop columns related to individual transactions, 
#   then group the data by StockCode, Description, and Country, 
#   and finally compute a single price for the product.

if "inventory" not in [x.tableName for x in spark.sql("""show tables""").collect()]:
  # print("Create inventory table")
  ( spark
   .read
   .csv( path=master_csv_path, header=True )
   .drop( "InvoiceNo", "InvoiceDate", "CustomerID" )
   .filter( "Quantity > 0")                  # This is some weird data, filter it out.
   .groupby( "StockCode", "Description", "Country" )
   .agg({"Quantity":"sum","UnitPrice":"max"})
   .withColumnRenamed("max(UnitPrice)","UnitPrice")
   .withColumnRenamed("sum(Quantity)", "Quantity")
   .write
   .format("parquet")
   .saveAsTable("inventory")
  )
else:
  # print("Inventory table already exists")
  pass

# Pass just to suppress the useless output
pass

# COMMAND ----------

import datetime
print( "Notebook setup complete:", datetime.datetime.now() )

# COMMAND ----------


