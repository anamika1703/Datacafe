# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is for the common code

# COMMAND ----------

#to read the data
import os
import sys
import time
import traceback
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import lit, concat, year, month, current_date, current_timestamp, date_format, to_timestamp
from pyspark import SparkContext, SparkConf
def load_data(source, path): 
	'''
		Reads function from given source

		Parameters::
		spark 		- Spark Session
		source		- fomat of the file to read
		path		- Any file source
	'''
	try:
		df = spark.read
		if source in ["csv", "json", "txt","parquet"]:
			df = df.format(source)\
					.option("inferSchema", "true")\
					.option("header", "true")\
					.load(path)

		if not isinstance(df, SparkDataFrame):
			raise TypeError("Failed to get df")

		return df 

	except Exception as e:
		raise TypeError("Failed to get df")

# COMMAND ----------

def common_utils_move(source, destination):
    dbutils.fs.mv(source,destination,recurse=True)
        
def common_utils_remove(source):
    dbutils.fs.rm(source,recurse=True)

def common_utils_copy(source, destination):
    dbutils.fs.cp(source,destination,recurse=True)

# COMMAND ----------

#function to write to files
def write_to_files(dataframe,fmt, location):
    if not isinstance(dataframe, SparkDataFrame):
        raise TypeError("Is not a SparkDataFrame")
        
    try:
        dataframe.coalesce(1).write.format(fmt).option("header","true").option("mode", "overwrite").save(location)
        
    except Exception as e:
        raise TypeError("Failed to write file")

# COMMAND ----------

#Below code is for autloader

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .trigger(once=True)
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

#auto loader query
def autoloader_query(format, source_format, table_name, checkpoint_directory):
    query = (spark.read \
            .format(format)\
            .option("inferSchema",True)\
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("header",True)\
            .load(path))
    return query

# COMMAND ----------

# to read the streaming data
def read_stream_data(source_format, source_path):
    query = (spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", source_format) \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load(source_path))
    return query

# COMMAND ----------

# MAGIC %md 
# MAGIC Cleansing rules

# COMMAND ----------

import json
from pyspark.sql.functions import *

def selectColsFromTable(db , tablename, columns, filter_cnd=''):
    query = f"select {columns} from {db}.{tablename}"
    if filter_cnd !='':
        query = f"select {columns} from {db}.{tablename} where {filter_cnd}"
    return query

def selectAllFromTable(db , tablename):
    query = f"select * from {db}.{tablename}"
    return query

def read_config(filename):
	fp = open(filename, 'r+')
	return json.loads(fp.read())

def time_conversion(df, time_col):
    df_with_time = df.withColumn("unix_timestamp", from_unixtime(substring(col(time_col),0,10),"yyyy-MM-dd HH:mm:ss"))
    df_with_time = df_with_time.withColumn("date", col("unix_timestamp").cast("date"))
    df_with_time = df_with_time.withColumn("hour", hour(col("unix_timestamp")))
    df_with_time = df_with_time.drop("unix_timestamp")
    return df_with_time

def join_tables(spark,logger,df1,df2,df1_key,df2_key,join_type):
        if(join_type=="inner"):
                        df_join=df1.join(df2,df1[df1_key]==df2[df2_key]).drop(df2[df2_key])
        return df_join

def filter_col(df,col_name,list_of_values):
        df=df.filter(col(col_name).isin(list_of_values))
        return df

def cast_columns(df,col_name,new_type):
    df_cast_prty=df.withColumn(col_name,df[col_name].cast(new_type))
    return df_cast_prty

def order_by_col(df,col):
        df_orderby_col=df.orderBy(df[col])
        return df_orderby_col

# COMMAND ----------

# MAGIC %md
# MAGIC dlt functions to clean the data

# COMMAND ----------

# #dlt functions


# def dlt_func(action, columnname, func):
#     try:
#         if action == "expectall":
#             @dlt.expect_all(columnname,func)
#         elif action == "expectordrop":
#             @dlt.expect_or_drop(columnname,func)
#         elif action = "expect_or_fail":
#             @dlt.expect_or_fail(columnname,func)
#         elif action = "expect":
#             @dlt.expect(columnname,func)  
#         elif action = "expect_all_or_drop":
#             @dlt.expect_all_or_drop(columnname,func)
#         else:
#             @dlt.expect_all_or_fail(columnname,func)   
#     except Exception as e:
#         raise TypeError("falied dlt function")
