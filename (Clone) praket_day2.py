# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# dbutils.fs.mount(

#   source = "wasbs://raw@praketstorage.blob.core.windows.net",
#   mount_point = "/mnt/praketstorage/raw",
#   extra_configs= 
# {"fs.azure.account.key.praketstorage.blob.core.windows.net":"7SktxAYfAbOQG0MhSK1AhLWlmYAYW1G9OqJQ4gtcByezchBwCvYnGrGxYolS76hOF6/6yDNoBto7+AStzWnWzg=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/praketstorage/raw

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/praketstorage/raw/json/")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/praketstorage/raw/json/praket/json").saveAsTable("json.bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.bronze

# COMMAND ----------


