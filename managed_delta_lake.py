# Databricks notebook source
# MAGIC %md #Managed Delta Lake - Inventory Data
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC 
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions and increased performance to Apache Spark™ and big data workloads.
# MAGIC * Transactions
# MAGIC * Schema Enforcement on the data lake
# MAGIC * Schema Evolution
# MAGIC * Time Travel (data versioning)
# MAGIC * Updates and Deletes
# MAGIC * Increased Read query performance
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## In this notebook, we will show 4 use cases:
# MAGIC 0. Updating data in a data lake vs. Delta Lake
# MAGIC 1. Deleting data from a data lake vs. Delta Lake
# MAGIC 2. Time Travel
# MAGIC 3. Optimize query performance with `OPTIMIZE` and `ZORDER`
# MAGIC 
# MAGIC We will be using a copy of the inventory sample dataset: `/databricks-datasets/online_retail/data-001/data.csv`

# COMMAND ----------

#%fs rm /user/don@databricks.com/golden_demos/delta/databricks-datasets/online_retail/data-001/data.csv

# COMMAND ----------

# DBTITLE 1,Notebook setup
# MAGIC %run ./includes/setup

# COMMAND ----------

# MAGIC %md # 1. Updating data in a data lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSERT and UPDATE in Parquet: 6-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert and update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 1. Identify the rows that will be replaced (i.e. updated)
# MAGIC 1. Identify all of the rows that are not impacted by the insert or update
# MAGIC 1. Create a new temp based on all three insert statements
# MAGIC 1. Delete the original table (and all of those associated files)
# MAGIC 1. Rename the temp table back to the original table name
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif"/>

# COMMAND ----------

# DBTITLE 1,Create a copy of the inventory table as a plain-old Parquet-backed table
# MAGIC %sql
# MAGIC create table inventory_parquet
# MAGIC   using parquet 
# MAGIC   as select * from inventory;

# COMMAND ----------

# DBTITLE 1,View inventory Parquet table
# MAGIC %sql
# MAGIC select * 
# MAGIC from inventory_parquet 
# MAGIC limit 5

# COMMAND ----------

# DBTITLE 1,1. New items to insert into inventory Parquet table
cols  = [ 'StockCode', 'Description',           'Quantity', 'UnitPrice', 'Country']
items = [('2187709',   'RICE COOKER',            30,        50.04,       'United Kingdom'), 
         ('2187631',   'PORCELAIN BOWL - BLACK', 10,        8.33,        'United Kingdom')
        ]

insert_items = spark.createDataFrame(items, cols)

display(insert_items)

# COMMAND ----------

# DBTITLE 1,2. Items to update in inventory Parquet table
cols =  ['StockCode', 'Description',        'Quantity', 'UnitPrice', 'Country']
items = [('21877',    'HOME SWEET HOME MUG', 300,       26.04,       'United Kingdom'), 
         ('21876',    'POTTERING MUG',       1000,      48.33,       'United Kingdom')]

update_items = spark.createDataFrame(items, cols)

display(update_items)

# COMMAND ----------

# DBTITLE 1,3. Unchanged items to retain in inventory Parquet table
unchanged_items = spark.sql("select * from inventory_parquet where StockCode !='21877' and StockCode != '21876'")

display(unchanged_items)

# COMMAND ----------

# DBTITLE 1,4. Create new temporary Parquet table with all the rows (unchanged, changed, new)
new_inventory = ( unchanged_items
                  .union(update_items)
                  .union(insert_items)
                )

( new_inventory
  .write
  .format("parquet")
  .saveAsTable("inventory_new_parquet")
)

# COMMAND ----------

# DBTITLE 1,5. Drop previous inventory Parquet table -- race condition!
# MAGIC %sql 
# MAGIC drop table inventory_parquet

# COMMAND ----------

# DBTITLE 1,6. Rename temp table to be main inventory Parquet table
# MAGIC %sql
# MAGIC alter table inventory_new_parquet rename to inventory_parquet

# COMMAND ----------

# MAGIC %md ## Okay, those 6 steps were not that bad!?
# MAGIC 
# MAGIC What if an failure happened during these 6 steps? 
# MAGIC 
# MAGIC **With OSS Spark can have incomplete results and no way to roll back and that adds to a lack of consistency**
# MAGIC 
# MAGIC By using Delta not only do we simplify the code, but with transaction we ensure either "all or nothing" actions

# COMMAND ----------

# MAGIC %md ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) INSERT or UPDATE with Databricks Delta
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`
# MAGIC 
# MAGIC Note: Additional statements available:  `MERGE`, `UPDATE`, `DELETE`

# COMMAND ----------

# DBTITLE 1,Create a copy of the inventory table as a Delta table
# MAGIC %sql 
# MAGIC create table inventory_delta
# MAGIC   using delta
# MAGIC   as select * from inventory;

# COMMAND ----------

# DBTITLE 1,View inventory Delta table
# MAGIC %sql
# MAGIC select * from inventory_delta limit 5

# COMMAND ----------

# DBTITLE 1,1. Items to insert OR update in inventory Delta table
cols  = ['StockCode', 'Description',           'Quantity', 'UnitPrice', 'Country']
items = [('2187709',  'RICE COOKER',            30,         50.04,      'United Kingdom'), 
         ('2187631',  'PORCELAIN BOWL - BLACK', 10,          8.33,      'United Kingdom'), 
         ('21877',    'HOME SWEET HOME MUG',    300,        26.04,      'United Kingdom'), 
         ('21876',    'POTTERING MUG',          1000,       48.33,      'United Kingdom')]

spark.createDataFrame(items, cols).createOrReplaceTempView("merge_items")

# COMMAND ----------

# DBTITLE 1,2. Use MERGE to insert and update the inventory Delta table
# MAGIC %sql
# MAGIC MERGE INTO inventory_delta as d
# MAGIC USING merge_items as m
# MAGIC on d.StockCode = m.StockCode and d.Country = m.Country
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Successfully merged into inventory Delta table!
# MAGIC %sql 
# MAGIC select * 
# MAGIC from inventory_delta 
# MAGIC where true
# MAGIC       and StockCode in ('2187709', '2187631', '21877', '21876') 
# MAGIC       and Country = 'United Kingdom'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delete
# MAGIC 
# MAGIC You can easily `DELETE` rows from delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cannot do delete records in  in Parquet

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from inventory_parquet where StockCode = '2187709';

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from inventory_parquet where StockCode = '2187709';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Successfully DELETE from Delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from inventory_delta where StockCode = '2187709';

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from inventory_delta where StockCode = '2187709';

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from inventory_delta where StockCode = '2187709';

# COMMAND ----------

# MAGIC %md ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Time Travel with Delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history inventory_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from inventory_delta VERSION as of 1
# MAGIC where StockCode = '2187709'

# COMMAND ----------

# MAGIC %md ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) OPTIMIZE and ZORDER
# MAGIC 
# MAGIC Improve your query performance with `OPTIMIZE` and `ZORDER` using file compaction and a technique to co-locate related information in the same set of files. This co-locality is automatically used by Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC Legend:
# MAGIC * Gray dot = data point e.g., chessboard square coordinates
# MAGIC * Gray box = data file; in this example, we aim for files of 4 points each
# MAGIC * Yellow box = data file that’s read for the given query
# MAGIC * Green dot = data point that passes the query’s filter and answers the query
# MAGIC * Red dot = data point that’s read, but doesn’t satisfy the filter; “false positive”
# MAGIC 
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png)
# MAGIC 
# MAGIC Reference: [Processing Petabytes of Data in Seconds with Databricks Delta](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

# COMMAND ----------

# DBTITLE 1,Run query on Delta
# MAGIC %sql 
# MAGIC select * from inventory_delta where Country = 'United Kingdom' and StockCode like '21%' and UnitPrice > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize inventory_delta
# MAGIC ZORDER by Country, StockCode

# COMMAND ----------

# DBTITLE 1,Run the same query (even faster)
# MAGIC %sql select * from inventory_delta where Country = 'United Kingdom' and StockCode like '21%' and UnitPrice > 10

# COMMAND ----------

# MAGIC %md
# MAGIC The query over Delta runs much faster after `OPTIMIZE` and `ZORDER` is run. How much faster the query runs can depend on the configuration of the cluster you are running on, however should be **5-10X** faster compared to the standard table. 

# COMMAND ----------

# DBTITLE 1,Clean up files with vacuum 
# MAGIC %sql VACUUM inventory_delta retain 168 HOURS
