# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "refined-layer").getOrCreate()

# COMMAND ----------

# MAGIC %run /Repos/vicnocrato@live.com/databricks-supply-chain/utils

# COMMAND ----------

utils = Utils()

# COMMAND ----------

# MAGIC %md
# MAGIC  spark.read.table("hive_metastore.raw.raw_customers_data").show()

# COMMAND ----------

df_customers = spark.read.table("hive_metastore.raw.raw_customers_data")

# COMMAND ----------

df_customers = df_customers.fillna("NAM",subset=["Customer_Region"])

# COMMAND ----------

df_customers = utils.remove_leading_zeros(df_customers,"Native_Customer")

# COMMAND ----------

# MAGIC %md
# MAGIC df_customers.show()

# COMMAND ----------

utils.create_table_identity("trusted_customer_id", "customer", "refine")
utils.create_table_identity("trusted_subregion", "subregion", "refine")

# COMMAND ----------

df_customers.select("Native_Customer").withColumnRenamed("Native_Customer", "customer").write.mode("overwrite").saveAsTable("refine.trusted_customer_id")

# COMMAND ----------

df_customers.select("Customer_Subregion").distinct().withColumnRenamed("Customer_Subregion", "subregion").write.mode("overwrite").saveAsTable("refine.trusted_subregion")

# COMMAND ----------

df_customer_id = spark.read.table("hive_metastore.refine.trusted_customer_id")
df_subregion_id = spark.read.table("hive_metastore.refine.trusted_subregion")
df_region_id = spark.read.table("hive_metastore.refine.trusted_region")
df_region_default = spark.read.table("hive_metastore.utils.region_default")
df_country = spark.read.table("hive_metastore.refine.trusted_country")

# COMMAND ----------

df_customers = df_customers.join(df_customer_id, df_customer_id.customer == df_customers.Native_Customer, "inner")\
    .join(df_subregion_id, df_subregion_id.subregion == df_customers.Customer_Subregion, "inner")\
             .join(df_country, df_country.country == df_customers.Customer_Country, "inner")\
              .select("customer_id", "country_id","subregion_id", "region_id")

# COMMAND ----------

df_customers.write.mode("overwrite").saveAsTable("refine.trusted_customer")
