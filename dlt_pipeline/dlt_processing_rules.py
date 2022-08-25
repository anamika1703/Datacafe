# Databricks notebook source
# MAGIC %pip install fernet

# COMMAND ----------

# MAGIC %pip install cryptography

# COMMAND ----------

from pyspark.sql.functions import *
@udf(StringType())
def encrypt_val(clear_text):
    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    f = Fernet(key)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text
spark.udf.register("encrypt_val", encrypt_val) # register the square udf for Spark SQL


# COMMAND ----------

def autoloader_query(format,path):
    return(
    spark.read \
            .format(format)\
            .option("inferSchema",True)\
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("header",True)\
            .load(path))

    

# COMMAND ----------

input_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/DataCafe/file_path.csv")
files={}
for row in input_data.collect():
    files[row['name']] = row['path']
    #print(row)

# COMMAND ----------

def get_rules_new(tag):
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("dbfs:/FileStore/DataCafe/rules.csv")
  fi=[]
  for row in df.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
  return rules
 
def get_rules_new1(tag):
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("dbfs:/FileStore/DataCafe/rules.csv")
  fi=[]
  for row in df.filter(col("tag") == tag).collect():
   
    st=row['name']
    st1=row['constraint']
    
    fi.append(row['name'])
    fi.append(st1)
    print(fi)
  return st,st1

# COMMAND ----------

import dlt
@dlt.table(name="customer_data",
                  comment = "The raw data for ICE Vehicles.",
  table_properties={
    "quality": "bronze_raw"
  }
   )
def customer_table():
    #df=autoloader_query("csv","dbfs:/DataCafe/source/customer_data")
    #df=df.withColumn("customer_name",hash(col("customer_name")))
    return(autoloader_query("csv","dbfs:/DataCafe/source/customer_data"))

@dlt.table(name="customer_masked",
                  comment = "The raw data for ICE Vehicles.",
  table_properties={
    "quality": "bronze_raw"
  }
   )
def customer_enc():
    df=dlt.read_stream("customer_data")
    df=df.withColumn("customer_name",encrypt_val(df["customer_name"]))
    return(df)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, hour, sum


#source = spark.conf.get("source")


    

all_tables = []

def generate_tables(call_table,path,format, response_table, merge_table,tag1,tag2,tag3,tag4,cus_ID,device_ID,silver_table,partition,gold_table_daily,gold_table_monthly):
  
 
  
  @dlt.table(
    name=call_table,
    comment="top level tables by call type"
  )
  
  def create_call_table():
    return (autoloader_query(format,path))
 
  @dlt.expect_or_drop(get_rules_new1(tag1)[0],get_rules_new1(tag1)[1])
  @dlt.expect_or_drop(get_rules_new1(tag2)[0],get_rules_new1(tag2)[1])
  @dlt.expect(get_rules_new1(tag3)[0],get_rules_new1(tag3)[1])
  @dlt.expect_all(get_rules_new(tag4))

  @dlt.table(
    name=response_table,
    comment="cleaned bronze table",
    #table_properties={
    #"pipelines.autoOptimize.zOrderCols": "transitionId"
  #}
   )
  def create_response_table():
    return (dlt.read_stream(call_table))
  dlt.create_streaming_live_table(merge_table)

  dlt.apply_changes(
  target = merge_table,
  source = response_table,
  keys = ["transitionId"],
  sequence_by = col("time"),
 
   )
   
  @dlt.view(name=silver_table,
                  comment = "The silver layer data.",
             )
  def create_silver_table():
      #bronze_one = dlt.read_stream(response_table)
      #bronze_two = dlt.read(customer_table)
      #bronze_two = bronze_two.select("time","AC","visibility","pressure","windSpeed","cloudCover")
      #bronze_two=bronze_two.withColumnRenamed("time","timestamp")
      
      return ( 
          spark.sql("""
          SELECT * FROM LIVE.customer_masked c 
          join LIVE.{merge_table} f 
          ON f.{cus_ID}=c.{device_ID}""".format(merge_table=merge_table,cus_ID=cus_ID,device_ID=device_ID)))
    
    # Joining the two Silver Tables by calling them by the "function" name
  @dlt.table(name=partition,
  partition_cols = [ 'date' ]
)
  def get_partition():
        df=dlt.read(silver_table)
        df=df.withColumn("unix_timestamp", concat_ws(".",from_unixtime(substring(col("time"),0,10),"yyyy-MM-dd HH:mm:ss"),substring(col("time"),-3,3)))
        df = df.withColumn("date", col("unix_timestamp").cast("date"))
    
        return ( df )
  @dlt.table(name=gold_table_daily,
                  comment = "The aggregated data.",
             )
  def create_gold_table():
        silver_one = dlt.read(partition)
        silver_one=silver_one.withColumn("unix_timestamp", concat_ws(".",from_unixtime(substring(col("time"),0,10),"yyyy-MM-dd HH:mm:ss"),substring(col("time"),-3,3)))

        extended = (silver_one
           .withColumn("date", col("unix_timestamp").cast("date"))
           .withColumn("hour", hour(col("unix_timestamp"))))
        
        
        if "fridge" in tag1:            return(extended.groupBy("date","custrefrigeratorId").agg(sum('powerConsumption').alias('powerConsumption'),sum('Milk5linPer').alias('Milk5linPer')))
        
        elif "alarm" in tag1:
            extended=extended.filter(extended.bedroom1=="1")
            extended=extended.filter(extended.bedroom2=="1")
            extended=extended.filter(extended.hall=="1")
            extended=extended.filter(extended.kitchen=="1")
            extended=extended.groupBy("date","hour","custalarmId")\
                             .agg(count('bedroom1')\
                             .alias('bedroom1_count')\
                             ,count('bedroom2').alias('bedroom2_count')\
                                  ,count('hall').alias('hall_count')\
                                  ,count('kitchen').alias('kitchen_count'))
       
            return(extended)
        elif "watch" in tag1:
            extended=extended.where(extended.sleepLog=="1")
            return(extended.groupBy("date","custwatchId").\
                 agg(sum('calories').alias('calories')\
                ,sum('stepTotal').alias('stepTotal')\
                ,sum('totalDistance').alias('totalDistance')))
          
      
    
        else:    
            return ( 
                 extended.groupBy("date",cus_ID)\
                .agg(sum('powerConsumption').alias('powerConsumption')))
  @dlt.table(name=gold_table_monthly,
                  comment = "The aggregated data.",
)
  def create_gold_monthly():
        silver_one = dlt.read(partition)
        
            
        silver_one=silver_one.withColumn("unix_timestamp", concat_ws(".",from_unixtime(substring(col("time"),0,10),"yyyy-MM-dd HH:mm:ss"),substring(col("time"),-3,3)))

        extended = (silver_one
            .withColumn("date", col("unix_timestamp").cast("date"))
                .withColumn("hour", hour(col("unix_timestamp")))
            .withColumn("month", month(col("unix_timestamp")))
             .withColumn("year", year(col("unix_timestamp"))))
        if "alarm" in tag1:
            return(extended.groupBy("year","month",cus_ID).agg(count('kitchen').alias('kitchen_alarm')))
            
        elif "watch" in tag1:
            extended=extended.where(extended.sleepLog=="1")
            return(extended.groupBy("date","hour","custwatchId").\
                 agg(sum('calories').alias('calories')\
                ,sum('stepTotal').alias('stepTotal')\
                ,sum('totalDistance').alias('totalDistance')))
            
        else:
            return ( extended.groupBy("year","month",cus_ID).agg(sum('powerConsumption').alias('powerConsumption')))
            
          
    
  all_tables.append(customer_table)
  all_tables.append(response_table)
  all_tables.append(merge_table)
  all_tables.append(silver_table)
  all_tables.append(gold_table_daily)
  all_tables.append(gold_table_monthly)
  

generate_tables("smart_geaser",files["smart_geaser"],"json","bronze_geaser_clean","smart_geaser_bronze_merged","validation_Geaser_ID","validation_Geaser_time","validation_Geaser_power","validation_geaser_param","custgeaserId","geaser_Id","smart_geaser_Silver","geaser_partition","smart_geaser_Gold_daily","smart_geaser_Gold_monthly")

#generate_tables("smart_ac",files["smart_ac"],"csv","bronze_ac_clean","smart_ac_bronze_merged","validation_AC_ID","validation_AC_time","validation_AC_power","validation_ac_param","custacId","ac_Id","smart_ac_Silver","ac_partition","smart_ac_Gold_daily","smart_ac_Gold_monthly")

#generate_tables("smart_refrigerator",files["smart_refrigerator"],"csv","bronze_refrigerator_clean","smart_refrigerator_bronze_merged","validation_fridge_ID","validation_fridge_time","validation_fridge_power","validation_fridge_param","custrefrigeratorId","refrigerator_Id","smart_regriferator_Silver","regriferator_partition","smart_regriferator_Gold_daily","smart_regriferator_Gold_monthly")
#generate_tables("smart_bulb",files["smart_bulb"],"csv","bronze_Bulb_clean","smart_Bulb_bronze_merged","validation_Bulb_ID","validation_Bulb_time","validation_Bulb_power","validation_Bulb_param","custbulbId","bulb_Id","smart_bulb_Silver","bulb_partition","smart_bulb_Gold_daily","smart_bulb_Gold_monthly")


#generate_tables("smart_alarm",files["smart_alarm"],"csv","bronze_alarm_clean","smart_alarm_bronze_merged","validation_alarm_ID","validation_alarm_time","validation_alarm_power","validation_alarm_param","custalarmId","alarm_Id","smart_alarm_Silver","alarm_partition","smart_alarm_Gold_daily","smart_alarm_Gold_monthly")

#generate_tables("smart_microwave",files["smart_microwave"],"json","bronze_microwave_clean","smart_microwave_bronze_merged","validation_microwave_ID","validation_microwave_time","validation_microwave_power","validation_microwave_param","custmicrowaveId","microwave_Id","smart_microwave_Silver","microwave_partition","smart_microwave_Gold_daily","smart_microwave_Gold_monthly")

#generate_tables("smart_washer",files["smart_washer"],"csv","bronze_washer_clean","smart_washer_bronze_merged","validation_washer_ID","validation_washer_time","validation_washer_power","validation_washer_param","custwasherId","washer_Id","smart_washer_Silver","washer_partition","smart_washer_Gold_daily","smart_washer_Gold_monthly")

#generate_tables("smart_watch",files["smart_watch"],"csv","bronze_watch_clean","smart_watch_bronze_merged","validation_watch_ID","validation_watch_time","validation_watch_calories","validation_watch_totalDistance","custwatchId","watch_Id","smart_watch_Silver","watch_partition","smart_watch_Gold_daily","smart_watch_Gold_hourly")

#generate_tables("smart_door",files["smart_door"],"parquet","bronze_door_clean","smart_door_bronze_merged","validation_door_ID","validation_door_time","validation_door_power","validation_door_param","custdoorId","door_Id","smart_door_Silver","door_partition","smart_door_Gold_daily","smart_door_Gold_hourly")









