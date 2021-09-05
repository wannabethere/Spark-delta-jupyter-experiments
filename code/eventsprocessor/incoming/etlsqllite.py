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


def transition_data(start_date, end_date):
    os.chdir(path + "/Transition/year=2021/month=07")
    #trans_con = sqlite3.connect("202107_transitions.db")
    #start my flexgrid container
    # first connect to the resource
    # discover schema
    # identify the pII/quasi pii columns
    # Get rules on data based on the license and Accountant access that the user has
    # read sql and apply row level as well as aggregate for privacy/deidentification/supression
    
    dataset describe, stats
    #EKS autoscaling soln
    #S3 files list (Assuming we get the assumerole permission for external accts)
    #folder structure we can build a partition by asking the user to define indexes 
    
    Assumption for the small startup we pick a distributed data store technology
    For ex: Redis
    
    Container (redis server --> we fetch the load into a table)
    
    trans_con = flexgrid_client_con("discover transitions data")
    
    First--> we check if the user has access to the container
    
    
    # print(trans_con)
    events_df = pd.read_sql(
        "SELECT * FROM transitions WHERE strftime('%Y-%m-%d', event_timestamp) >= '" + start_date + "' AND  strftime('%Y-%m-%d', event_timestamp) <= '" + end_date + "'",
        trans_con)

    print(events_df)

    return events_df


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
        
