# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name

# COMMAND ----------

landinginventorysnapshotdir0 = "dbfs:/mnt/pcupos"
landinginventorysnapshotdir1 = "dbfs:/mnt/pcupos"
landinglatestinventorysnapshotdir0 = "dbfs:/mnt/pcupos"
landinglatestinventorysnapshotdir1 = "dbfs:/mnt/pcupos"

base_data_dir="/mnt/pcupos"
check_point_dir="/mnt/pcupos/checkpoint"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

inventory_schema = StructType([StructField('trans_id', StringType()), 
StructField('store_id', IntegerType()), 
StructField('date_time', TimestampType()), 
StructField('change_type_id', IntegerType()), 
StructField('items', ArrayType( 
StructType([ StructField('item_id', IntegerType()),  
StructField('quantity', IntegerType()) 
 ]) 
)) 
]) 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

inventory_snapshot_schema = StructType([
    StructField('id', IntegerType()),
    StructField('item_id', IntegerType()),
    StructField('employee_id', IntegerType()),
    StructField('store_id', IntegerType()),
    StructField('date_time', TimestampType()),
    StructField('quantity', IntegerType())
])

# COMMAND ----------


from pyspark.sql.types import *

@dlt.table(
    name='inventory_change_store',
    comment='This Table reads inventory files ',
    table_properties={'quality':'bronze'}    
    )
#@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def read_IOT_device():
   # Set the IoT Hub Event Hub-compatible endpoint details
    event_hub_ns = " " #
    #TOPIC = "$Default"
    TOPIC = "p" # The name of the IOT HUB created under IOT HUB
    #connection_string = """nnnnnn"""
    connection_string = """nnnnnn""" # # This is the event hub compatible endpoint property of the iot hub created in azure iot hub(under Hub settings);just click on the build-in end points created to retreive various information including the event hub compatible endpoint inputed here.
    BOOTSTRAP_SERVERS = f"{event_hub_ns}.servicebus.windows.net:9093"
    EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{connection_string};\";"

    # Read IoT Hub messages from Event Hub-compatible endpoint using Kafka format
    return (
                spark.readStream \
                .format("kafka") \
                .option("subscribe", TOPIC) \
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
                .option("kafka.sasl.mechanism", "PLAIN") \
                .option("kafka.security.protocol", "SASL_SSL") \
                .option("kafka.sasl.jaas.config", EH_SASL) \
                .option("kafka.request.timeout.ms", "240000") \
                .option("kafka.session.timeout.ms", "240000") \
                .option("failOnDataLoss", "false") \
                .option("startingOffsets", "earliest") \
                .load()
            )
    
    
@dlt.table(
    name='inventory_snapshot_store0',
    comment='This Table reads inventory files ',
    table_properties={'quality':'bronze'}    
    )
def read_inventory_snapshot_store0():    
    return (
            spark.readStream.format("text").schema(inventory_snapshot_schema)
            .load(f"{base_data_dir}/pos/generator/inventory_snapshot_online")
            .withColumn("InputFile", input_file_name())
        )

@dlt.table(
    name='inventory_snapshot_store1',
    comment='This Table reads inventory files ',
    table_properties={'quality':'bronze'}    
    )
def read_inventory_snapshot_store1():    
    return (
            spark.readStream.format("text").schema(inventory_snapshot_schema)
            .load(f"{base_data_dir}/pos/generator/inventory_snapshot_store001")
            .withColumn("InputFile", input_file_name())
        )

# Function to read the latest inventory snapshot for store 0
@dlt.table(
    name='latest_inventory_snapshot_store0',
    comment='This Table reads inventory files ',
    table_properties={'quality':'bronze'}    
    )
def read_latest_inventory_snapshot_store0():    
    return (
            spark.readStream.format("csv").schema(inventory_snapshot_schema)
            .load(f"{base_data_dir}/pos/inventory_snapshots/inventory_snapshot_0_2021-01-01 00:00:00")
            .withColumn("InputFile", input_file_name())
        )

# Function to read the latest inventory snapshot for store 1
@dlt.table(
    name='latest_inventory_snapshot_store1',
    comment='This Table reads inventory files ',
    table_properties={'quality':'bronze'}    
    )
def read_latest_inventory_snapshot_store1():    
    return (
            spark.readStream.format("csv").schema(inventory_snapshot_schema)
            .load(f"{base_data_dir}/pos/inventory_snapshots/inventory_snapshot_1_2021-01-01 00:00:00")
            .withColumn("InputFile", input_file_name())
        )


# COMMAND ----------

dbutils.fs.ls('/mnt/pcupos/pos/inventory_snapshots/')


# COMMAND ----------

from pyspark.sql.functions import col, from_json, explode, abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F

import dlt



@dlt.table(
    name='cleaned_inventory_change',
    comment='This Table is cleaned inventory tables for inventory records, Append only orders with valid timestamps',
    table_properties={'quality':'Bronze_to_Silver ETL'}
)
#@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def cast_iot_data():
    return (dlt.read_stream("inventory_change_store").select(F.from_json(F.col("value").cast("string"), inventory_snapshot_schema).alias("value"))
          .select("value.*"))

def flattened_iot_data():
    return (cast_iot_data()
            .withColumn("items", explode(col("value.items")))
            .select("value.trans_id", "value.store_id", "value.date_time", 
                    "value.change_type_id", "items.item_id", "items.quantity"))

def drop_duplicates_flattened_iot_data():
    return flattened_iot_data().dropDuplicates()

def absolute_inventory():
    return drop_duplicates_flattened_iot_data().withColumn("quantity", abs(col("quantity")))


@dlt.table (
    name='cleaned_inventory_snapshot',
    comment='This Table is cleaned inventory tables for inventory records, Append only orders with valid timestamps',
    table_properties={'quality':'silver'}
)
def cleaned_inventory_snapshot():
    return (
        dlt.read("inventory_snapshot_store0").alias("a")
            .join(
                dlt.read("inventory_snapshot_store1").alias("b"), 
                on="item_id",
                how='left'
            )
             # Rename conflicting columns after the join
            .selectExpr("a.*", 
                        "b.quantity as quantity_b", 
                        "b.date_time as date_time_b", 
                        "b.employee_id as employee_id_b", 
                        "b.store_id as store_id_b")
    )

@dlt.table (
    name='cleaned_latest_inventory_snapshot',
    comment='This Table is cleaned inventory tables for inventory records, Append only orders with valid timestamps',
    table_properties={'quality':'silver'}
)
def cleaned_latest_inventory_snapshot():
    return (
        dlt.read("latest_inventory_snapshot_store0").alias("c")
            .join(
                dlt.read("latest_inventory_snapshot_store1").alias("d"), 
                on="item_id",
                how='left'
            )
            # Renaming the conflicting columns after the join to avoid duplicates
            .selectExpr("c.*", 
                        "d.quantity as quantity_d", 
                        "d.date_time as date_time_d", 
                        "d.employee_id as employee_id_d", 
                        "d.store_id as store_id_d",
                        "d.InputFile as InputFile_d")
    )



# COMMAND ----------

import dlt
from pyspark.sql.functions import sum, max

@dlt.table(
    name='inventory_current_agg',
    comment='This Table is aggregate table for the current inventory ',
    table_properties={'quality':'Silver-to-Gold ETL'},
    )

def getAggregates():    
    return (
        dlt.read("cleaned_inventory_change").alias("f")
        .join(dlt.read("cleaned_inventory_snapshot").alias("g"), on="item_id")
        .join(dlt.read("cleaned_latest_inventory_snapshot").alias("h"), on="item_id")
        .groupBy("f.store_id", "f.item_id")
        .agg(
            sum("f.quantity").alias("TotalQuantity"),
            max("f.date_time").alias("latest_date_time")
        )
    )

