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

df_recov = spark.sql("select min(id) as id,plant,ferm_code,run_time,machine_volume_L,bactch_output_g_l from raw.raw_recovery_data group by plant,ferm_code,run_time,machine_volume_L,bactch_output_g_l ")
df_comp = spark.read.table("hive_metastore.raw.raw_products_components")
df_trans = spark.read.table("hive_metastore.raw.raw_ferm_transportation")
df_pl_id = spark.read.table("hive_metastore.refine.trusted_plant_id")
df_pl = spark.read.table("hive_metastore.refine.trusted_plant")
df_semi = spark.read.table("hive_metastore.refine.trusted_semi_product")
df_reg = spark.read.table("hive_metastore.refine.trusted_region")
df_make = spark.read.table("hive_metastore.raw.raw_make_site")

# COMMAND ----------

utils.create_table_identity("trusted_ferm_id","ferm_code","refine")

# COMMAND ----------

df_recov.select("ferm_code").distinct().write.mode("overwrite").saveAsTable("refine.trusted_ferm_id")

# COMMAND ----------

df_ferm_id = spark.read.table("hive_metastore.refine.trusted_ferm_id")

# COMMAND ----------

df_recov.join(df_pl_id, df_recov.plant == df_pl_id.plant,"inner").join(df_ferm_id, df_ferm_id.ferm_code == df_recov.ferm_code).select("plant_id", "ferm_code_id", "run_time", "machine_volume_L", "bactch_output_g_l").dropna().withColumnRenamed("bactch_output_g_l", "batch_output_g_L").write.mode("overwrite").saveAsTable("refine.trusted_ferm")

# COMMAND ----------

df_ferm_id = spark.read.table("hive_metastore.refine.trusted_ferm_id")

# COMMAND ----------

df_comp = df_comp.join(df_semi, df_comp.Semi_Product_Code == df_semi.semi_product, "inner").join(df_reg, df_reg.region == df_comp.Region).join(df_ferm_id, df_ferm_id.ferm_code == df_comp.Comp1).select("semi_product_id", "no_of_Components", "ferm_code_id","Comp2","Comp3","Comp4","Comp5","Comp6", "region_id").withColumnRenamed("ferm_code_id","Comp1")
df_comp = df_comp.join(df_ferm_id, df_ferm_id.ferm_code == df_comp.Comp2,"left").select("semi_product_id", "no_of_Components", "Comp1","ferm_code_id","Comp3","Comp4","Comp5","Comp6", "region_id").withColumnRenamed("ferm_code_id","Comp2")
df_comp = df_comp.join(df_ferm_id, df_ferm_id.ferm_code == df_comp.Comp3,"left").select("semi_product_id", "no_of_Components", "Comp1","Comp2","ferm_code_id","Comp4","Comp5","Comp6", "region_id").withColumnRenamed("ferm_code_id","Comp3")
df_comp = df_comp.join(df_ferm_id, df_ferm_id.ferm_code == df_comp.Comp4,"left").select("semi_product_id", "no_of_Components", "Comp1","Comp2","Comp3","ferm_code_id","Comp5","Comp6", "region_id").withColumnRenamed("ferm_code_id","Comp4")
df_comp = df_comp.join(df_ferm_id, df_ferm_id.ferm_code == df_comp.Comp5,"left").select("semi_product_id", "no_of_Components", "Comp1","Comp2","Comp3","Comp4","ferm_code_id","Comp6", "region_id").withColumnRenamed("ferm_code_id","Comp5")
df_comp = df_comp.join(df_ferm_id, df_ferm_id.ferm_code == df_comp.Comp6,"left").select("semi_product_id", "no_of_Components", "Comp1","Comp2","Comp3","Comp4","Comp5","ferm_code_id", "region_id").withColumnRenamed("ferm_code_id","Comp6")

# COMMAND ----------

df_comp = df_comp.unpivot(["semi_product_id","no_of_Components","region_id"],["Comp1","Comp2","Comp3","Comp4","Comp5","Comp6"],"comp_number","ferm_code_id").withColumnRenamed("no_of_Components","total_no_components").dropna()

# COMMAND ----------

df_make = df_make.select("Semi_Product_Code","Ferm_Code", "gL_Factor", "region").distinct()

# COMMAND ----------

df_make = df_make.join(df_reg, df_make.region == df_reg.region,"inner").join(df_semi, df_make.Semi_Product_Code == df_semi.semi_product,"inner").join(df_ferm_id, df_ferm_id.ferm_code == df_make.Ferm_Code,"inner").select("semi_product_id","ferm_code_id","region_id","gl_Factor").withColumnRenamed("gl_Factor","gl_factor")

# COMMAND ----------

df_comp = df_comp.join(df_make, (df_comp.semi_product_id == df_make.semi_product_id) & (df_comp.region_id == df_make.region_id) & (df_comp.ferm_code_id == df_make.ferm_code_id)).sort(df_comp.semi_product_id,df_comp.region_id,df_comp.comp_number).select(df_comp.semi_product_id,df_comp.total_no_components,df_comp.region_id,df_comp.comp_number,df_comp.ferm_code_id,df_make.gl_factor)

# COMMAND ----------

df_comp.write.mode("overwrite").saveAsTable("refine.trusted_recipes")

# COMMAND ----------

df_trans.join(df_pl_id, df_pl_id.plant == df_trans.plant_id).join(df_ferm_id, df_ferm_id.ferm_code == df_trans.Ferm_Code).select("refine.trusted_plant_id.plant_id","g_L_ferm_density","ferm_code_id").write.mode("overwrite").saveAsTable("refine.trusted_ferm_density")
