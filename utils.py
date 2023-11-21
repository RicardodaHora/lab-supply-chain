# Databricks notebook source
# MAGIC %md
# MAGIC # This a utils notebook
# MAGIC ### Functions
# MAGIC list_tables_in_db(db_name)
# MAGIC
# MAGIC find_tables_with_column_in_db(column,db)
# MAGIC
# MAGIC import_data(schema,table_name)
# MAGIC
# MAGIC insert_data_parquet(df,table_name)
# MAGIC
# MAGIC insert_data(table,table_name,schema)
# MAGIC
# MAGIC remove_blank_spaces_column(df)

# COMMAND ----------

from abc import abstractmethod
from re import search
spark = SparkSession.builder.appName("utils").getOrCreate()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
 


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

class Utils:
    """
        This module contains methods to support process in other notebooks
    """

    @abstractmethod
    def list_tables_in_db(self, db_name: str):
        """ this method list all tables in a database"""
        return spark.sql(f"show tables from {db_name}").rdd.map(lambda x: x[1]).collect()
    
    @abstractmethod
    def find_tables_with_column_in_db(self, column: str, db: str):
        """ this method checks if a column exists in the database and if it does list the tables that contain the column """
        utils = Utils()
        list_ = utils.list_tables_in_db(db)
        tables_with_column = []
        for table in list_:
            df = spark.read.table(db+"."+table)
            for item in df.toPandas().columns.to_list(): 
                if search(column,item.lower()):
                    tables_with_column.append(table)
        return tables_with_column if tables_with_column != [] else f"no tables with column {column}"
    
    @abstractmethod
    def import_data(self, schema: str, table_name: str):
        """ this method import a table from a sql database """
        sql_tables = spark.read.jdbc(url=jdbcUrl, table=schema + "." + table_name, properties=connectionProperties)
        return sql_tables
    
    @abstractmethod
    def insert_data_parquet(self, df, table_name: str):
        df.write.mode('overwrite').parquet(f"/Repos/vicnocrato@live.com/databricks-supply-chain/Data/{table_name}.parquet")

    @abstractmethod
    def insert_data(self, table,table_name,schema):
        table.write.saveAsTable(name=schema + "." + table_name,mode = "overwrite")

    @abstractmethod
    def remove_blank_spaces_column(self, df):
        df = df.toPandas()
        df.columns = df.columns.str.replace(" ","_")
        df = spark.createDataFrame(df)
        return df

    def import_default_region(self):
        self.dataframe = spark.read.table("utils.region_default")
    
    def default_region(self, region: str):
        return self.dataframe.where(self.dataframe.region == region).select(self.dataframe.default).rdd.map(lambda x: x[0]).collect()
    
    def create_table_identity(self, table_name, column_name, db):
        spark.sql(f"create or replace table {db}.{table_name} ({column_name}_id BIGINT GENERATED ALWAYS AS IDENTITY,{column_name} STRING)")

    def remove_leading_zeros(self, df, column):
        return df.withColumn(column, F.regexp_replace(column, r'^[0]*', ''))

