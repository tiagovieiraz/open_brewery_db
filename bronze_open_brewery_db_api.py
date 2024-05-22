# Databricks notebook source
# MAGIC %md
# MAGIC #Installing open brewery lib

# COMMAND ----------

pip install openbrewerydb

# COMMAND ----------

# MAGIC %md
# MAGIC #Importing required libs

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp, xxhash64, concat_ws
from delta.tables import * 

# COMMAND ----------

# MAGIC %md
# MAGIC #Getting API data and declaring df schema

# COMMAND ----------

url = "https://api.openbrewerydb.org/breweries"

response = requests.get(url)
data = response.json()

schema = StructType(fields=[ 
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('brewery_type', StringType(), True),
    StructField('address_1', StringType(), True),
    StructField('address_2', StringType(), True),
    StructField('address_3', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state_province', StringType(), True),
    StructField('postal_code', StringType(), True),
    StructField('country', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('phone', StringType(), True),
    StructField('website_url', StringType(), True),
    StructField('state', StringType(), True),
    StructField('street', StringType(), True)
    ])

df = spark.createDataFrame(data, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Transformation
# MAGIC Adding a timestamp for data insertion control.

# COMMAND ----------

  df = df.withColumn("insertion_at", current_timestamp())

# COMMAND ----------

tableFullName = 'bronze.open_brewery_db.breweries_list'

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating if not exists delta table

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
  .tableName(tableFullName) \
  .addColumns(df.schema) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC #Saving Delta Table

# COMMAND ----------

df.write.mode("append").saveAsTable(f"{tableFullName}")
