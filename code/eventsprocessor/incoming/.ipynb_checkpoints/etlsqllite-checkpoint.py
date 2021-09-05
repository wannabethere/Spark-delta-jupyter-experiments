from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col, expr,udf,to_timestamp, from_json,date_format,row_number
import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from delta.tables import DeltaTable
import shutil
import json
import datetime, time
from random import randint
from csv2sqllite import * 


kafka_broker = "broker:29092"
topicName = "test"


#            "spark.jars": "sqlite-jdbc-3.8.11.2.jar"
def session():
    # Enable SQL commands and Update/Delete/Merge for the current spark session.
    # we need to set the following configs .partitionBy(tableInfo.partitionBy) \
    spark = SparkSession.builder \
        .appName("quickstart") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.jars", "sqlite-jdbc-3.8.11.2.jar") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.2,org.xerial:sqlite-jdbc:jar:3.31.1') \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate() 
    return spark

def write(url,tablename,df):
    df.write.jdbc(url=url, table=tablename, mode="append", properties={"driver":"org.sqlite.JDBC"})
    return

def writeOneFile(df,location):    
    df.repartition(1).write.option("header","true").mode("overwrite").csv(location)    
    return
    
def readFiles(spark,basePath,listoflocations):   
    df=spark.read.option("header", "true").option("basePath",basePath).csv(listoflocations) 
    return df

def etlsqllite(spark,basePath,srclist,table):
    df = readFiles(spark,basePath,srclist)
    
    writeOneFile(df,basePath+"/comb/"+table)
    return


year = 2021
months= ['06','07']

if __name__ == '__main__':
    spark = session()
    paths = []
    pathstransitions= []
    for month in months:
        paths.append(f'/opt/workspace/clickstream/events/year={year}/month={month}/*/*.csv')
        pathstransitions.append(f'/opt/workspace/clickstream/transitions/year={year}/month={month}/*/*.csv')
        eventsLocation = f"events/year={year}/month={month}/"
        transitionsLocation = f'transitions/year={year}/month={month}/'
        print(paths)
        print(eventsLocation)
        etlsqllite(spark,'/opt/workspace/clickstream/',paths,eventsLocation)
        etlsqllite(spark,'/opt/workspace/clickstream/',pathstransitions,transitionsLocation)
        
