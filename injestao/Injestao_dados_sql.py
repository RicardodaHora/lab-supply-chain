# Databricks notebook source
# MAGIC %md
# MAGIC ## Importação das libs

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName( "pandas to spark").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do string de Conexação
# MAGIC
# MAGIC Obs: Utilizar secrets e keys para user e password

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

# MAGIC %md
# MAGIC ### Receber lista de tabelas do banco

# COMMAND ----------

sql_tables = spark.read.jdbc(url=jdbcUrl, table="(select name from sys.tables where schema_name(schema_id) = 'raw') as t", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação de dicionario De-Para SQL - Databricks

# COMMAND ----------

sql_tables.toPandas()['name']

# COMMAND ----------

sql_tables_list = sql_tables.toPandas()['name'].sort_values().to_list()
databricks_name_list =["raw_forecast_demand_data",
                       "raw_inventory_data",
                       "raw_finish_products",
                       "raw_historical_data",
                       "raw_plants_data",
                       "raw_customers_data",
                       "raw_products_components",
                       "raw_semi_finish_data",
                       "raw_ferm_transportation",
                       "raw_freight",
                       "raw_make_site",
                       "raw_plant_capacity",
                       "raw_recovery_data",
                      ]

depara_sql_databricks = dict(zip(sql_tables_list, databricks_name_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções de importação e escrita das tabelas
# MAGIC
# MAGIC table : dataframe a ser salvo no databricks
# MAGIC
# MAGIC table_name : nome da tabela. Import_data : nome da tabela a ser importada do SQL. Insert_data: nome da tabela no databricks

# COMMAND ----------

def import_data(table_name):
    sql_tables = spark.read.jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)
    return sql_tables

def insert_data(table,table_name,schema):
    table.write.saveAsTable(name=schema + "." + table_name,mode = "overwrite")

def remove_blank_spaces_column(df):
    df = df.toPandas()
    df.columns = df.columns.str.replace(" ","_")
    df = spark.createDataFrame(df)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importação e ingestão da lista de tabelas

# COMMAND ----------

for row in sql_tables.collect():
    df = import_data('raw.'+ row['name'])
    df = remove_blank_spaces_column(df)
    insert_data(df,depara_sql_databricks[row['name']],"raw")

# COMMAND ----------


