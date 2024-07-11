# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CDCTrial1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

table_path = "/tmp/delta-table"
data = [(1, "John", 30), (2, "Doe", 25), (3, "Alice", 28)]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").save(table_path)
spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

df.display()

# COMMAND ----------

data = [(4, "Bob", 23)]
df2 = spark.createDataFrame(data, columns)
df2.write.format("delta").mode("append").save(table_path)

# COMMAND ----------

df2.display()

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, table_path)
delta_table.update(
    condition="id == 2",
    set={"age": "30"}
)

# COMMAND ----------

updated_df = spark.read.format("delta").load(table_path)
print("Updated Table:")
updated_df.show()

# COMMAND ----------

change_data_stream = spark.readStream.format("delta").option("readChangeData", "true").option("startingVersion", 0).load("/tmp/delta-table")
query = change_data_stream.writeStream \
    .format("console") \
    .option("checkpointLocation", "/tmp/delta-table-checkpoints") \
    .start()

# COMMAND ----------

data = [(5, "Bobby", 23)]
df_1 = spark.createDataFrame(data, columns)
df_1.write.format("delta").mode("append").save(table_path)

# COMMAND ----------

query.awaitTermination(60)

# COMMAND ----------

query.stop()

# COMMAND ----------

change_data= spark.read.format("delta").option("readChangeData", "true").option("startingVersion", 1).load("/tmp/delta-table")
change_data.show()

# COMMAND ----------


