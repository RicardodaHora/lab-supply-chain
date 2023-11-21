# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "refined-layer").getOrCreate()

# COMMAND ----------

# MAGIC %run /Repos/vicnocrato@live.com/databricks-supply-chain/utils

# COMMAND ----------

utils = Utils()

# COMMAND ----------

utils.list_tables_in_db("raw")

# COMMAND ----------

list_ = utils.find_tables_with_column_in_db("region","raw")
print (list_)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.raw_customers_data where Customer_Region is null

# COMMAND ----------

customers_region = spark.sql("select distinct Customer_Region from raw.raw_customers_data")
freight_region = spark.sql("select distinct Shipto_Region as region from raw.raw_freight union select distinct Ship_from_Region as region from raw.raw_freight")
make_site_region = spark.sql("select distinct region from raw.raw_make_site")
prod_comp_region = spark.sql("select distinct Region from raw.raw_products_components")

# COMMAND ----------

print(customers_region.show())
print(freight_region.show())
print(make_site_region.show())
print(prod_comp_region.show())

# COMMAND ----------

customers_region = customers_region.fillna("NAM")
customers_region.show()

# COMMAND ----------

list_ = [customers_region,freight_region,make_site_region,prod_comp_region]

# COMMAND ----------

list_region = []
for table in list_:
    list_region.extend(table.rdd.map(lambda x: x[0]).collect())

list_region = list(set(list_region))

# COMMAND ----------

# MAGIC %md
# MAGIC default_region = {"A":"ASPAC", "L": "LAM", "N": "NAM", "E": "EMEA"}

# COMMAND ----------

# MAGIC %md
# MAGIC region_dict = {}
# MAGIC for region in list_region:
# MAGIC     region_dict[region] = default_region[region[0]]
# MAGIC region_dict

# COMMAND ----------

# MAGIC %md
# MAGIC create database utils

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.types import StructType,StructField,StringType
# MAGIC df = spark.createDataFrame(region_dict.items(), 
# MAGIC                       schema=StructType(fields=[
# MAGIC                           StructField("region", StringType()), 
# MAGIC                           StructField("default", StringType())]))
# MAGIC
# MAGIC utils.insert_data(df, "region_default","utils")

# COMMAND ----------

# MAGIC %md
# MAGIC utils.import_default_region()

# COMMAND ----------

# MAGIC %md
# MAGIC utils.default_region("NA")

# COMMAND ----------

# MAGIC %md
# MAGIC make_site_region.show()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE refine.trusted_region (
# MAGIC   region_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   region STRING
# MAGIC );

# COMMAND ----------

make_site_region.write.mode('overwrite').saveAsTable("refine.trusted_region")

# COMMAND ----------


