#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/11 21:13
# @Author  : 
# @Site    : 
# @File    : 
# @Software: 

'''
receive data coming from kafka (producer.py) and insert data into delta table
'''
# debug spark—submit by pycharm (Chinese): https://blog.csdn.net/zj1244/article/details/78893837
# (English): https://stackoverflow.com/questions/35560767/pyspark-streaming-with-kafka-in-pycharm

import json, os, re

from delta.tables import *

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import json
import global_vals


kafka_broker = "my.kafka.queue:9092"
bucket_prefix = "my-company-bucket-prefix-"

class RawEventsTable:
  __init__(self,eventName,spark,location,eventPartiton):
    #validate spark connection
    self.spark = spark
    with open('data.txt') as json_file:
      self.schema = json.load(location+eventName+"/"+"schema.json")
    self.topic = topic
    self.delta_location = location+eventName+"/table/"
    self.checkpoint_location = location+eventName +"/checkpoint/"
    self.partitionCol= eventPartiton


  def createschema(self, df):
    return (self.spark
     .createDataFrame([], self.schema)
     .write
     .option("mergeSchema", "true")
     .format("delta")
     .partitionBy("date")
     .mode("append")
     .save(self.delta_location))

  def upsertToDelta(self,df, batch_id): 
    (DeltaTable
    .forPath(self.spark, self.delta_location)
    .alias("t")
    .merge(df.alias("s"), "s.kafka_key = t.kafka_key")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

  def writeToDelta(self,df):
    w = df.writeStream;
    if not streaming:
      w = w.trigger(once=True)
    #Todo: test .trigger(continuous='10 second')  
    (w.format("delta")
      .option("checkpointLocation", self.checkpoint_location) 
      .foreachBatch(self.upsertToDelta) 
      .outputMode("update") 
      .start(self.delta_location)) 

  def read_stream_kafka_topic(self):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_broker)
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            # filter out empty values
            .withColumn("value", expr("string(value)"))
            .filter(col("value").isNotNull())
            .select(
              # offset must be the first field, due to aggregation
              expr("offset as kafka_offset"),
              expr("timestamp as kafka_ts"),
              expr("string(key) as kafka_key"),
              "value"
            )
            # get latest version of each record
            .select("kafka_key", expr("struct(*) as r"))
            .groupBy("kafka_key")
            .agg(expr("max(r) r"))
            # convert to JSON with schema
            .withColumn('value', 
                        from_json(col("r.value"), self.schema))
            .select('r.kafka_key', 
                    'r.kafka_offset', 
                    'r.kafka_ts', 
                    'value.*'
            ))
  
  def shutdown(self):
    # wait for streaming to finish
    while spark.streams.active != []:
      time.sleep(10)

  def optimize(self):
    sql = "OPTIMIZE delta.`{}`".format(delta_location)
    spark.sql(sql)
    None    
    
def str_to_bool(value):
  FALSE_VALUES = ['false', 'no', '0']
  TRUE_VALUES = ['true', 'yes', '1']
  lvalue = str(value).lower()
  if lvalue in (FALSE_VALUES): return False
  if lvalue in (TRUE_VALUES):  return True
  raise Exception("String value should be one of {}, but got '{}'.".format(
    FALSE_VALUES + TRUE_VALUES, value))


def validate_required_argument_and_return_value(name):
  value = getArgument(name)
  if len(value) < 1:
    dbutils.notebook.exit("'{}' argument value is required.".format(name))
  return value