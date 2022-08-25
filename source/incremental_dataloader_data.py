# Databricks notebook source
# MAGIC %run /DataCafe/common/common_code

# COMMAND ----------

load_type = dbutils.widgets.get("load_type")
if load_type == "historical":
    dbutils.notebook.exit("will run the historical loop.")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
import os
import glob
from datetime import datetime  
from datetime import timedelta


SOURCE_PATH = "/dbfs/FileStore/DataCafe/common_file/source_data.json"
source_data = read_config(SOURCE_PATH)
expectationsdf = load_data("csv","dbfs:/FileStore/DataCafe/audit/event_log")
expectationsdf = expectationsdf.filter(col("status") == 'COMPLETED').orderBy(col("date_timestamp").desc())
last_date = expectationsdf.select("date_timestamp").collect()[0][0]
modified_date = last_date + timedelta(days=1)
modified_date = modified_date.strftime("%Y-%m-%d")
last_successful_date = last_date.strftime("%Y-%m-%d")
last_month = last_date.strftime("%m-%d")

for json_dict in source_data:

    if json_dict['stream_data'] == 'Y':
        source_df = load_data(json_dict['file_format'],json_dict['input_path'])
        source_df = time_conversion(source_df,"time")
        input_data = source_df.where((source_df.date >= last_successful_date) & (source_df.date <= modified_date))
        #input_data.show(5,False)
        write_to_files(input_data,json_dict['file_format'],"dbfs:/FileStore/DataCafe/temp/")       
        file_path=glob.glob("/dbfs/FileStore/DataCafe/temp/*."+json_dict['file_format'])[0]
        file_path1=os.path.basename(file_path)
        common_utils_move("dbfs:/FileStore/DataCafe/temp/"+file_path1,json_dict['input_stream_path']+"/"+json_dict['name']+last_month+"."+json_dict['file_format'])
        common_utils_remove("dbfs:/FileStore/DataCafe/temp/")
