# Databricks notebook source
# MAGIC %md
# MAGIC #Creating silver table if not exists

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.open_brewery_db.breweries_list(
    id string,
    name string,
    brewery_type string, 
    address_1 string,
    address_2 string,
    address_3 string,
    city string,
    state_province string,
    postal_code string,
    country string,
    longitude double,
    latitude double,
    phone string,
    website_url string,
    state string,
    street string,
    insertion_at timestamp
)""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Getting the most recent data from bronze layer

# COMMAND ----------

df = spark.sql("""
 SELECT 
    a.*
    FROM 
        bronze.open_brewery_db.breweries_list a
    WHERE 
        a.insertion_at = (SELECT MAX(insertion_at) FROM bronze.open_brewery_db.breweries_list)""")

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformations
# MAGIC Casting "latitude" and "longitude" columns to Double type.

# COMMAND ----------

df = df.withColumn("longitude", col("longitude").cast(DoubleType())) \
       .withColumn("latitude", col("latitude").cast(DoubleType()))

# COMMAND ----------

tableFullName = "silver.open_brewery_db.breweries_list"

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing silver delta table partioning by location (state and city)

# COMMAND ----------

df.write.format("delta").mode("overwrite").partitionBy("state", "city").saveAsTable(tableFullName)
