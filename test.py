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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable
import shutil
import json
import os


kafka_broker = "localhost:9092"
bucket_prefix = "my-company-bucket-prefix-"
topicName = "test"

def session():
    # Enable SQL commands and Update/Delete/Merge for the current spark session.
    # we need to set the following configs
    spark = SparkSession.builder \
        .appName("quickstart") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def writeStream(df,tableInfo):
    df.writeStream \
        .format("delta") \
        .partitionBy(tableInfo.partitionBy) \    
        .outputMode("append") \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", "/mnt/datalake/gps/_checkpoints/kafka") \
        .start("/mnt/datalake/gps")

def readStream(spark, tableInfo):
    df= spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topicName) \
        .option("startingOffsets", "latest") \
        .load()
    return df    


class RAWEVENTSTABLE:
    def __init__(self,eventName, location):
       self.eventName = eventName
       self.checkpointlocation = location + eventName + "/checkpoint_location/"
       self.tableLocation = location + eventName + "/events/"
       self.partitionBy = "date"
    



