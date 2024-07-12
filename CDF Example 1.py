# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CDF_Table_Example") \
    .getOrCreate()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS CDF_Example;
# MAGIC USE SCHEMA CDF_Example;
# MAGIC CREATE TABLE IF NOT EXISTS CDF_Example.Salary_Account (
# MAGIC     Id INT,
# MAGIC     Name STRING,
# MAGIC     Salary INT
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO CDF_Example.Salary_Account (id, name, Salary) VALUES
# MAGIC (1, 'Venkata Sai', 65000),
# MAGIC (2, 'Mallikarjuna Rao', 80000)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS CDF_Example.CDF_Target_Table (
# MAGIC     Id INT,
# MAGIC     Name STRING,
# MAGIC     Salary INT
# MAGIC )
# MAGIC USING delta
# MAGIC

# COMMAND ----------

checkpoint_path = "/tmp/checkpoints/change_feed_example"

spark.readStream.format("delta") \
  .option("readChangeFeed", "true") \
  .table("Salary_Account") \
  .writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_path) \
  .option("mergeSchema", "true") \
  .trigger(availableNow=True) \
  .table("CDF_Target_Table") 



# COMMAND ----------

import time
spark.sql("USE SCHEMA CDF_Example")
for i in range(3, 10):
    spark.sql(f"""
    INSERT INTO CDF_Example.Salary_Account (Id, Name, Salary) VALUES
    ({i}, 'Name{i}', {i * 1000})
    """)
    # time.sleep(5) 


# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM CDF_Example.Salary_Account;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CDF_Example.CDF_Target_Table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('CDF_Example.Salary_Account', 1)
# MAGIC ORDER BY _commit_timestamp;

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS CDF_Example.Salary_Account;
# DROP TABLE IF EXISTS CDF_Example.CDF_Target_Table;
# DROP DATABASE IF EXISTS CDF_Example;

# COMMAND ----------

# dbutils.fs.rm(checkpoint_path, recurse=True)
