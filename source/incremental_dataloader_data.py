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
now = datetime.now()
if now != last_date:
    modified_date = last_date + timedelta(days=1)
    modified_date = modified_date.strftime("%Y-%m-%d %H:%M:%S")
else:
    modified_date = now + timedelta(hours=1)
    modified_date = modified_date.strftime("%Y-%m-%d %H:%M:%S")
last_successful_date = last_date.strftime("%Y-%m-%d")
last_month = last_date.strftime("%m-%d")

modified_date = modified_date.strftime("%Y-%m-%d %H:%M:%S")
dt_string = now.strftime("%d%m%Y%H%M%S")

for json_dict in source_data:

    if json_dict['stream_data'] == 'Y':
        source_df = load_data(json_dict['file_format'],json_dict['input_path'])
        source_df = time_conversion(source_df,"time")
        input_data = source_df.where((expectationsdf.date_timestamp >= modified_date) & (expectationsdf.date_timestamp <= last_successful_date))
        write_to_files(input_data,json_dict['file_format'],"dbfs:/FileStore/DataCafe/temp/")       
        file_path=glob.glob("/dbfs/FileStore/DataCafe/temp/*."+json_dict['file_format'])[0]
        file_path1=os.path.basename(file_path)
        common_utils_move("dbfs:/FileStore/DataCafe/temp/"+file_path1,json_dict['input_stream_path']+"/"+json_dict['name']+last_month+"."+json_dict['file_format'])
        common_utils_remove("dbfs:/FileStore/DataCafe/temp/")
