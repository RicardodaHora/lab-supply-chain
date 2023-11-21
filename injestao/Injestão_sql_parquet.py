# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "parquet_ingestion").getOrCreate()

# COMMAND ----------

jdbcHostname = "vmourasql.database.windows.net:1433"
jdbcDatabase = "vmourasqlsupplychain"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://vmourasql.database.windows.net:1433;database=vmourasqlsupplychain"
connectionProperties = {\
"user":"CloudSA2256d215@vmourasql",
"password":"**Venezia1897",
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
"encrypt":"true",
"trustServerCertificate":"false",
"hostNameInCertificate":"*.database.windows.net",
"loginTimeout": "30"
}

# COMMAND ----------

sql_tables = spark.read.jdbc(url=jdbcUrl, table="(SELECT name FROM sys.tables) as t", properties=connectionProperties)

# COMMAND ----------

sql_tables_list = sql_tables.toPandas()['name'].to_list()
databricks_name_list =["raw_recovery_data",
                       "raw_semi_finish_data",
                       "raw_plant_capacity",
                       "raw_make_site",
                       "raw_freight",
                       "raw_ferm_transportation",
                       "raw_products_components",
                       "raw_historical_data",
                       "raw_finish_products",
                       "raw_inventory_data",
                       "raw_forecast_demand_data",
                       "raw_customers_data",
                       "raw_plants_data"]

depara_sql_databricks = dict(zip(sql_tables_list, databricks_name_list))

# COMMAND ----------

def import_data(schema,table_name):
    sql_tables = spark.read.jdbc(url=jdbcUrl, table=schema + "." + table_name, properties=connectionProperties)
    return sql_tables

def insert_data_parquet(df,table_name):
    df.write.mode('overwrite').parquet(f"/Repos/vicnocrato@live.com/databricks-supply-chain/Data/{table_name}.parquet")

# COMMAND ----------

for row in sql_tables.collect():
    df = import_data("raw",row['name'])
    insert_data_parquet(df,depara_sql_databricks[row['name']])

# COMMAND ----------

display(spark.read.parquet("/Repos/vicnocrato@live.com/databricks-supply-chain/Data/raw_recovery_data.parquet"))
