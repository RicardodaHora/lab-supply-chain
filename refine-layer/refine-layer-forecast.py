# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "refined-layer").getOrCreate()
from pyspark.sql.functions import when


# COMMAND ----------

# MAGIC %run /Repos/vicnocrato@live.com/databricks-supply-chain/utils

# COMMAND ----------

utils = Utils()

# COMMAND ----------

df_demand = spark.read.table("hive_metastore.raw.raw_forecast_demand_data")
df_fini = spark.read.table("hive_metastore.refine.trusted_fini_product_code")
df_pl = spark.read.table("hive_metastore.refine.trusted_plant_id")

# COMMAND ----------

df_demand = df_demand.join(df_fini, df_demand.Finish_Product_Code == df_fini.fini_product, "inner").join(df_pl, df_pl.plant == df_demand.Plant, "inner").select("Calendar_Year_Month",df_pl.plant_id,df_fini.fini_product_id, "Native_Soldto_Customer", "Native_Shipto_Customer", "Consensus_Forecast_KG").withColumnRenamed("Calendar_Year_Month", "date").withColumnRenamed("Consensus_Forecast_KG","forecast_kg")

# COMMAND ----------

df_demand.show()

# COMMAND ----------

df_cus_id = spark.read.table("hive_metastore.refine.trusted_customer_id")
df_cus = spark.read.table("hive_metastore.refine.trusted_customer")

# COMMAND ----------

df_demand = df_demand.join(df_cus_id, df_demand.Native_Shipto_Customer == df_cus_id.customer, "inner").select("date","plant_id","fini_product_id","Native_Soldto_Customer", "customer_id","forecast_kg")

# COMMAND ----------

df_demand = df_demand.join(df_cus, df_cus.customer_id == df_demand.customer_id).select(df_demand.date,df_demand.plant_id,df_demand.fini_product_id,df_demand.Native_Soldto_Customer,df_demand.forecast_kg,df_cus.country_id)

# COMMAND ----------

df_demand = df_demand.join(df_cus_id, df_cus_id.customer == df_demand.Native_Soldto_Customer, "inner") \
.select("date","plant_id", "fini_product_id","customer_id", "country_id","forecast_kg")

# COMMAND ----------

df_demand.groupBy("date","plant_id","fini_product_id","customer_id","country_id").agg(sum("forecast_kg").alias("forecast_kg")).sort("date").dropna().write.mode("overwrite").saveAsTable("refine.trusted_demand_forecast")
