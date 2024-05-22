# Databricks notebook source
# MAGIC %md
# MAGIC #Creating gold view aggregating by type and location

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace view vw_breweries_type_location as
# MAGIC select
# MAGIC     brewery_type,
# MAGIC     state,
# MAGIC     city,
# MAGIC     count(*) as brewery_count
# MAGIC from
# MAGIC     silver.open_brewery_db.breweries_list
# MAGIC group by
# MAGIC       all
