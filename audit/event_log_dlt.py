# Databricks notebook source
#%run /DataCafe/common/common_code

# COMMAND ----------

# to read the streaming data
def read_stream_data(source_format, source_path):
    query = (spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", source_format) \
      .option("cloudFiles.inferColumnTypes", "true") \
      .load(source_path))
    return query

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(name="bronze_event_log",
                  comment = "The event log data.",
  table_properties={
    "quality": "bronze_table"
  }
)
def event_logBronze():
    return (
    read_stream_data("csv","dbfs:/FileStore/DataCafe/audit_stream_test/event_log/"))

# COMMAND ----------

@dlt.table(name="bronze_refrigerator_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                        
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_refrigerator_clean') 
    )

# COMMAND ----------

@dlt.table(name="bronze_geaser_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                        
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_geaser_clean') 
    )

# COMMAND ----------

@dlt.table(name="bronze_alarm_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_alarm_clean')
    )

# COMMAND ----------

@dlt.table(name="bronze_ac_clean_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                        
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_ac_clean') 
    )

# COMMAND ----------

@dlt.table(name="bronze_Bulb_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                         
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_Bulb_clean')
    )

# COMMAND ----------

@dlt.table(name="bronze_watch_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                       
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_watch_clean')
    )

# COMMAND ----------

@dlt.table(name="bronze_door_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                       
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_door_clean')  
    )

# COMMAND ----------

@dlt.table(name="bronze_washer_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                         
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_washer_clean')
    )

# COMMAND ----------

@dlt.table(name="bronze_microwave_event_log",
                  comment = "The joined data.",
)
def gold_event_table():
    bronze_one = dlt.read_stream("bronze_event_log")
                         
    return ( 
     bronze_one.filter(col("dataset") == 'bronze_microwave_clean')
    )
