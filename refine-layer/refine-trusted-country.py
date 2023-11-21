# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "refined-layer-trusted-country").getOrCreate()

# COMMAND ----------

# MAGIC %run /Repos/vicnocrato@live.com/databricks-supply-chain/utils

# COMMAND ----------

utils = Utils()

# COMMAND ----------

list_ = utils.find_tables_with_column_in_db("country","raw")
print (list_)

# COMMAND ----------

df_customers = spark.read.table("raw.raw_customers_data")
df_plants = spark.read.table("raw.raw_plants_data")

# COMMAND ----------

df_customers.filter(df_customers.Customer_Country == "US").show()

# COMMAND ----------

df_customers = df_customers.select("Customer_Country", "Customer_Region").distinct().fillna("NAM")

# COMMAND ----------

df_customers.filter(df_customers.Customer_Country == "US").show()

# COMMAND ----------

df_customers.show()

# COMMAND ----------

df_plants.show()

# COMMAND ----------

df_region_default = spark.read.table("utils.region_default")

# COMMAND ----------

df_trusted_region = spark.read.table("refine.trusted_region")

# COMMAND ----------

df_country = df_customers.join(df_region_default, df_customers.Customer_Region == df_region_default.region, "left")\
    .select("Customer_Country","default").withColumnRenamed("default","region")

# COMMAND ----------

df_country = df_country.join(df_trusted_region, df_country.region == df_trusted_region.region, "left").select("Customer_Country", "region_id").withColumnRenamed("Customer_Country","country")

# COMMAND ----------

df_country = df_country.sort("country")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE refine.trusted_country (
# MAGIC   country_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   country STRING,
# MAGIC   region_id bigint
# MAGIC );

# COMMAND ----------

df_country.write.mode('overwrite').saveAsTable("refine.trusted_country")

# COMMAND ----------


