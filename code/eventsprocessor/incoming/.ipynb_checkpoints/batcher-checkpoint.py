#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/11 21:13
# @Author  : MengnanChen
# @Site    : 
# @File    : receiver.py
# @Software: PyCharm Community Edition

'''
receive data coming from kafka (producer.py) and insert data into mongodb
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

def createschema(df):
    return (spark
     .createDataFrame([], df.schema)
     .write
     .option("mergeSchema", "true")
     .format("delta")
     .mode("append")
     .save(delta_location))

def read_stream_kafka_topic(topic, schema):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_broker)
            .option("subscribe", topic)
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
                        from_json(col("r.value"), topic_schema))
            .select('r.kafka_key', 
                    'r.kafka_offset', 
                    'r.kafka_ts', 
                    'value.*'
            ))

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