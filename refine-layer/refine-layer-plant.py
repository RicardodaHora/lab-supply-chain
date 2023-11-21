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

df_plants = spark.read.table("hive_metastore.raw.raw_plants_data").distinct()

# COMMAND ----------

df_plants = utils.remove_leading_zeros(df_plants, "plant").distinct()

# COMMAND ----------

utils.create_table_identity("trusted_plant_id", "plant", "refine")

# COMMAND ----------

df_plants.select("Plant").withColumnRenamed("Plant","plant").distinct().write.mode("overwrite").saveAsTable("refine.trusted_plant_id")

# COMMAND ----------

df_country = spark.read.table("hive_metastore.refine.trusted_country")

# COMMAND ----------

df_plants = df_plants.withColumnRenamed("Plant","plant").join(df_country, df_country.country == df_plants.Country, "inner")
df_plants = df_plants.select("plant","country_id")

# COMMAND ----------

prod_list = spark.sql("select distinct Producing_Plant from raw.raw_make_site").toPandas()['Producing_Plant'].to_list()
fini_list = spark.sql("select distinct Finish_Plant from raw.raw_make_site").toPandas()['Finish_Plant'].to_list()
wh_list = spark.sql("select distinct Plant from raw.raw_forecast_demand_data").toPandas()['Plant'].to_list()

# COMMAND ----------

df_plants = df_plants.withColumn("is_prod", when(df_plants.plant.isin(prod_list),1).otherwise(0))
df_plants = df_plants.withColumn("is_fini", when(df_plants.plant.isin(fini_list),1).otherwise(0))
df_plants = df_plants.withColumn("is_wh", when(df_plants.plant.isin(wh_list),1).otherwise(0))

# COMMAND ----------

df_plant_id = spark.read.table("hive_metastore.refine.trusted_plant_id")

# COMMAND ----------

df_plants.join(df_plant_id, df_plant_id.plant == df_plants.plant, "inner").select("plant_id","country_id", "is_prod", "is_fini", "is_wh").write.mode("overwrite").saveAsTable("refine.trusted_plant")

# COMMAND ----------


