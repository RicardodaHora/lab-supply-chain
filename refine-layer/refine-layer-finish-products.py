# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "refined-layer").getOrCreate()

# COMMAND ----------

# MAGIC %run /Repos/vicnocrato@live.com/databricks-supply-chain/utils

# COMMAND ----------

utils = Utils()

# COMMAND ----------

df_finish_products = spark.read.table("raw.raw_finish_products")

# COMMAND ----------

df_fini_code = df_finish_products.select("Finish_Product_Code").distinct()
df_strategi_bus = df_finish_products.select("Document_Strategic_Business_Unit").distinct()
df_divisions = df_finish_products.select("Document_Divisions").distinct()
df_bus_unit = df_finish_products.select("Document_Business_Unit").distinct()
df_prod_group = df_finish_products.select("Document_Product_Group").distinct()
df_semi_prod_code = df_finish_products.select("Semi_Product_Code").distinct()

# COMMAND ----------

def create_table_identity(table_name,column_name):
    spark.sql(f"create or replace table refine.{table_name} ({column_name}_id BIGINT GENERATED ALWAYS AS IDENTITY,{column_name} STRING)")

# COMMAND ----------

create_table_identity("trusted_fini_product_code","fini_product")
create_table_identity("trusted_strategic_bu","strategic_bu")
create_table_identity("trusted_division","division")
create_table_identity("trusted_bu","bu")
create_table_identity("trusted_product_group","product_group")
create_table_identity("trusted_semi_product","semi_product")


# COMMAND ----------

# MAGIC %md
# MAGIC df_fini_code.show()

# COMMAND ----------

df_fini_code = df_fini_code.withColumnRenamed("Finish_Product_Code","fini_product")
df_strategi_bus = df_strategi_bus.withColumnRenamed("Document_Strategic_Business_Unit","strategic_bu")
df_divisions = df_divisions.withColumnRenamed("Document_Divisions","division")
df_bus_unit = df_bus_unit.withColumnRenamed("Document_Business_Unit","bu")
df_prod_group = df_prod_group.withColumnRenamed("Document_Product_Group","product_group")
df_semi_prod_code = df_semi_prod_code.withColumnRenamed("Semi_Product_Code","semi_product")

# COMMAND ----------

df_fini_code.write.mode('overwrite').saveAsTable("refine.trusted_fini_product_code")
df_strategi_bus.write.mode('overwrite').saveAsTable("refine.trusted_strategic_bu")
df_divisions.write.mode('overwrite').saveAsTable("refine.trusted_division")
df_bus_unit.write.mode('overwrite').saveAsTable("refine.trusted_bu")
df_prod_group.write.mode('overwrite').saveAsTable("refine.trusted_product_group")
df_semi_prod_code.write.mode('overwrite').saveAsTable("refine.trusted_semi_product")

# COMMAND ----------

spark.sql("create or replace table refine.trusted_finish_products (fini_product_id bigint,semi_product_id bigint,strategic_bu_id bigint,division_id bigint,bu_id bigint,product_group_id bigint)")

# COMMAND ----------

# MAGIC %md
# MAGIC df_finish_products.show()

# COMMAND ----------

df_fini = spark.read.table("refine.trusted_fini_product_code")
df_semi_prod = spark.read.table("refine.trusted_semi_product")
df_stra_bu = spark.read.table("hive_metastore.refine.trusted_strategic_bu")
df_divi = spark.read.table("hive_metastore.refine.trusted_division")
df_bu = spark.read.table("hive_metastore.refine.trusted_bu")
df_prod_gr = spark.read.table("hive_metastore.refine.trusted_product_group")

# COMMAND ----------

df_finish_products = df_finish_products.join(df_fini, df_finish_products.Finish_Product_Code == df_fini.fini_product, "inner").join(df_semi_prod, df_finish_products.Semi_Product_Code == df_semi_prod.semi_product_id,"inner").join(df_stra_bu,df_stra_bu.strategic_bu == df_finish_products.Document_Strategic_Business_Unit,"inner").join(df_divi,df_divi.division == df_finish_products.Document_Divisions,"inner").join(df_bu,df_bu.bu == df_finish_products.Document_Business_Unit,"inner").join(df_prod_gr,df_prod_gr.product_group == df_finish_products.Document_Product_Group,"inner")

# COMMAND ----------

df_finish_products.select("fini_product_id", "semi_product_id", "strategic_bu_id", "division_id", "bu_id", "product_group_id").write.insertInto("refine.trusted_finish_products")
