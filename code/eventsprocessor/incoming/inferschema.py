from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import json
import global_vals

infer_schema = update_kafka_schema

if not infer_schema:
  try:
      topic_schema_txt = dbutils.fs.head(schema_location)
  except:
    infer_schema = True
    pass

if infer_schema:
  topic_schema_txt = infer_topic_schema_json(topic)
  dbutils.fs.rm(schema_location)
  dbutils.fs.put(schema_location, topic_schema_txt)

def infer_topic_schema_json(topic):
    df_json = (spark.read
               .format("kafka")
               .option("kafka.bootstrap.servers", kafka_broker)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .option("failOnDataLoss", "false")
               .load()
               # filter out empty values
               .withColumn("value", expr("string(value)"))
               .filter(col("value").isNotNull())
               # get latest version of each record
               .select("key", expr("struct(offset, value) r"))
               .groupBy("key").agg(expr("max(r) r")) 
               .select("r.value"))
    
    # decode the json values
    df_read = spark.read.json(
      df_json.rdd.map(lambda x: x.value), multiLine=True)
    
    # drop corrupt records
    if "_corrupt_record" in df_read.columns:
        df_read = (df_read
                   .filter(col("_corrupt_record").isNotNull())
                   .drop("_corrupt_record"))
 
    return df_read.schema.json()
