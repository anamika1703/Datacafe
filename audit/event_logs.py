# Databricks notebook source
# MAGIC %run /DataCafe/common/common_code

# COMMAND ----------

# MAGIC %python 
# MAGIC #Filtering for each expectation no of records passed and failed
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC event_log = spark.read.format('delta').load(f"/pipelines/0dd457d5-d2a7-4927-8e18-f4152ed3dcbd/system/events")
# MAGIC  
# MAGIC expectationsdf = (event_log
# MAGIC        .selectExpr("id",
# MAGIC                    "timestamp",
# MAGIC                    "details:flow_progress.metrics.num_output_rows",
# MAGIC                    "details:flow_progress.data_quality.dropped_records",
# MAGIC                    "details:flow_progress.status",
# MAGIC                    "details:flow_progress.data_quality.expectations")
# MAGIC        .filter("event_type = 'flow_progress' and details:flow_progress.status ='COMPLETED'")
# MAGIC )
# MAGIC 
# MAGIC schema = ArrayType(StructType([
# MAGIC     StructField('name', StringType()),
# MAGIC     StructField('dataset', StringType()),
# MAGIC     StructField ('passed_records' , LongType()),
# MAGIC     StructField ('failed_records' , LongType())
# MAGIC ]))
# MAGIC expectationsdf = expectationsdf.withColumn("expectations", explode(from_json("expectations",schema)))
# MAGIC expectationsdf = expectationsdf.withColumn("date_timestamp", substring(col("timestamp"),0,19))
# MAGIC expectationsdf = expectationsdf.withColumn("dt", to_date(col("timestamp"),'YYYY-MM-DD'))
# MAGIC expectationsdf = expectationsdf.withColumn("tm", substring(col("timestamp"),12,8))
# MAGIC  
# MAGIC expectationsdf = (
# MAGIC   expectationsdf.selectExpr("id",
# MAGIC                             "date_timestamp",
# MAGIC                             "dt",
# MAGIC                             "tm",
# MAGIC                             "expectations.name",
# MAGIC                             "expectations.dataset",
# MAGIC                             "num_output_rows",
# MAGIC                             "dropped_records",
# MAGIC                             "expectations.passed_records",
# MAGIC                             "expectations.failed_records",
# MAGIC                             "status"))
# MAGIC expectationsdf.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
import os
import glob
from datetime import datetime,timedelta

now = datetime.now()
modified_date = now + timedelta(hours=1)
modified_date = modified_date.strftime("%Y-%m-%d %H:%M:%S")
last_successful_date = now.strftime("%Y-%m-%d %H:%M:%S")
dt_string = now.strftime("%d%m%Y%H%M%S")

input_data = expectationsdf
#.where((expectationsdf.date_timestamp >= modified_date) & (expectationsdf.date_timestamp <= last_successful_date))
input_data.show(5,False)
write_to_files(input_data,"csv","dbfs:/FileStore/DataCafe/audit_temp/")       
file_path=glob.glob("/dbfs/FileStore/DataCafe/audit_temp/*.csv")[0]
file_path1=os.path.basename(file_path)
common_utils_move("dbfs:/FileStore/DataCafe/audit_temp/"+file_path1,"dbfs:/FileStore/DataCafe/audit_stream_test/event_log/event_log_"+dt_string+".csv")
common_utils_remove("dbfs:/FileStore/DataCafe/audit_temp/")

# COMMAND ----------

query = autoload_to_table(data_source = "dbfs:/FileStore/DataCafe/audit_stream/event_log",
                          source_format = "csv",
                          table_name = "event_logs_gold",
                          checkpoint_directory = "dbfs:/FileStore/audit_target_table")
