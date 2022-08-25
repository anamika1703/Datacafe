# Databricks notebook source
# MAGIC %run /DataCafe/common/common_code

# COMMAND ----------

load_type = dbutils.widgets.get("load_type")
if load_type == "incremental":
    dbutils.notebook.exit("will run the incremental loop.")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
import os
import glob

SOURCE_PATH = "/dbfs/FileStore/DataCafe/common_file/sources_data.json"
source_data = read_config(SOURCE_PATH)
job_run_flag = load_data("csv","dbfs:/FileStore/DataCafe/common_file/job_run_flag")
job_run_flag = job_run_flag.orderBy(col("month"))
job_run_flag_month=job_run_flag.filter(job_run_flag.flag.isNull())
run_month=job_run_flag_month.select("month").collect()[0][0]
print(run_month)
if run_month < 10:
    month = '0'+str(run_month)
else:
    month = run_month
print(month)
for json_dict in source_data:

    if json_dict['stream_data'] == 'Y':
        source_df = load_data(json_dict['file_format'],json_dict['input_path'])
        source_df = time_conversion(source_df,"time")
        input_data=source_df.filter(f"date >='2022-{month}-01' AND date <='2022-{month}-31'")
        input_data = input_data.drop("date","hour")
        input_data.show(5,False)
        write_to_files(input_data,json_dict['file_format'],"dbfs:/FileStore/DataCafe/temp/")       
        file_path=glob.glob("/dbfs/FileStore/DataCafe/temp/*."+json_dict['file_format'])[0]
        file_path1=os.path.basename(file_path)
        common_utils_move("dbfs:/FileStore/DataCafe/temp/"+file_path1,json_dict['input_stream_path']+"/"+json_dict['name']+"_"+str(month)+"."+json_dict['file_format'])
        common_utils_remove("dbfs:/FileStore/DataCafe/temp/")
        
job_run_flag_month=job_run_flag_month.drop("flag")
job_run_flag_month=job_run_flag_month.withColumn("flag",when(col("month")==f"{run_month}","Done").otherwise(None))
job_run_flag_month.show()
write_to_files(job_run_flag_month,"csv","dbfs:/FileStore/DataCafe/common_file/job_run_flag_temp")
common_utils_remove("dbfs:/FileStore/DataCafe/common_file/job_run_flag")
common_utils_move("dbfs:/FileStore/DataCafe/common_file/job_run_flag_temp","dbfs:/FileStore/DataCafe/common_file/job_run_flag")
