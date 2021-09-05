
from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import col, expr,udf,to_timestamp, from_json,date_format,row_number
import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from delta.tables import DeltaTable
import shutil
import json
import datetime, time
from random import randint


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


def delete(tableInfo):
    # Clear any previous runs
    try:
        shutil.rmtree(tableInfo.tableLocation)
    except:
        pass
    return

def optimize(spark, tableInfo):
    sql = "OPTIMIZE delta.`{}`".format(tableInfo.tableLocation)
    spark.sql(sql)
    return
    
def getSparkSchemaFromJson(schema_json):
    new_schema = StructType.fromJson(json.loads(schema_json))
    return new_schema

#(x + datetime.timedelta(seconds = -x.second)
datemincol = udf(lambda x: x.strftime("%m%d%Y%H"), StringType())
#Currently hardcoding to 10 partitions
partitioncol = udf(lambda x: int(x)%10 if x.isdigit() else randint(0, 10), IntegerType())



class RawEventsTableForeachWriter:
    def open(self, partition_id, epoch_id):
        # Open connection. This method is optional in Python.
        pass

    def process(self, row):
        # Write row to connection. This method is NOT optional in Python.
        pass

    def close(self, error):
        # Close the connection. This method in optional in Python.
        pass

    
def raw_events_foreach_batch_function(batchdf, epoch_id):
    # Transform and write batchDF
    #.foreach(RawEventsTableForeachWriter()) #here we encrypt origcols, add noise to geo location, anonymize ip, user and other things 
    batchdf.persist()
    batchdf.write.format("delta") \
      .partitionBy("year","month","day") \
      .mode("append") \
      .save("/opt/workspace/raw/clickstream/events")
    
    #create the raw events processor
    batchdf.select("event_timestamp as date","usessionId as sessionId","eventtype as page_url_path","user_domain_id as userId","userPart as userPart","usessionId as sessionPart") \
    .write \
    .format("csv") \
    .mode("append") \
    .save("/opt/workspace/raw/clickstream/transitions")
    
   
    #create an node or vertex list for partitions of userid, sessionid and date of event
    #window  = Window.partitionBy("userId").orderBy(["userId","event_timestamp"])
    #df=df.withColumn('next_event', fn.lag('event', 1).over(window)).withColumn('next_event_timestamp', fn.lag('event_timestamp', 1).over(window))
    batchdf.unpersist()
    pass    

def raw_events_foreach_batch_function_json(batchdf, epoch_id, rawtableconfig):
    # Transform and write batchDF
    #.foreach(RawEventsTableForeachWriter()) #here we encrypt origcols, add noise to geo location, anonymize ip, user and other things 
    batchdf.persist()
    #create the raw events processor
    batchdf.persist()
    batchdf.write.format("csv") \
      .partitionBy("year","month","day") \
      .mode("append") \
      .save(rawtableconfig.tableLocation,header = 'true')
        
    batchdf=batchdf.select(col('sessiontime').alias('sessiontime'),fn.to_date(col(rawtableconfig.columnMapping['timestamp'])).alias('date'), \
                           col('usessionId').alias('sessionId') ,col(rawtableconfig.columnMapping['state']).alias('event'), col(rawtableconfig.columnMapping['userid']).alias('userId'), \
                           col('userPart').alias('userPart'), col('part').alias('sessionPart'), col('event_id').alias('eventId'), \
                           col('year').alias('year'), col('month').alias('month'), col('day').alias('day'), \
                           col(rawtableconfig.columnMapping['timestamp']).alias('event_timestamp'),col(rawtableconfig.columnMapping['eventtype']).alias('eventtype'))
 
    window  = Window.partitionBy("userId").orderBy(["userId","sessiontime"])
    batchdf=batchdf.withColumn('next_event', fn.lag('event', -1).over(window)).withColumn('next_event_timestamp', fn.lag('event_timestamp', -1).over(window)).\
                    withColumn('next_eventId', fn.lag('eventId', -1).over(window))
    batchdf.write \
    .partitionBy("year","month","day") \
    .format("csv") \
    .mode("append") \
    .save(rawtableconfig.location + rawtableconfig.eventName + "/transitions",header = 'true')
    
    
    #batchdf.write.jdbc(url=url, table="test", mode="overwrite", properties={"driver":"org.sqlite.JDBC"})
    #create an node or vertex list for partitions of userid, sessionid and date of event
    #window  = Window.partitionBy("userId").orderBy(["userId","event_timestamp"])
    #df=df.withColumn('next_event', fn.lag('event', 1).over(window)).withColumn('next_event_timestamp', fn.lag('event_timestamp', 1).over(window))
    batchdf.unpersist()
    pass    

class RawEventsTable:
    
    def __init__(self,location,eventName, schema):
       self.eventName = eventName
       self.location = location
       self.checkpointLocation = location + eventName + "/checkpoint_location/"
       self.tableLocation = location + eventName + "/events/"
       self.transitions = location + eventName + "/transitions/"
       self.partitionBy = "event_timestamp"
       self.configLocation =location + eventName + "/config/config.json" 
       with open(self.configLocation) as f:
           data = json.load(f)
       self.config = data 
       print(self.config['requiredcolumns']) 
       self.eschema = schema 
       self.usetimestamp = False
       self.columnMapping = {}


    
    def createStateTable(self,spark):
        # Create table in the metastore
        # TODO: Add a check for DelaTable.forPath() and check if the table exists
        
        DeltaTable.createIfNotExists(spark) \
            .tableName("transitions") \
            .addColumn("date", TimestampType()) \
            .addColumn("sessionId", StringType()) \
            .addColumn("eventType", StringType()) \
            .addColumn("userId", StringType()) \
            .addColumn("userPart", IntegerType()) \
            .addColumn("sessionPart", IntegerType()) \
            .addColumn("datePart", DateType(), generatedAlwaysAs="CAST(date AS DATE)") \
            .partitionedBy("userPart","datePart","sessionPart") \
            .location(self.transitions) \
            .execute()
        return


    def readStream(self,spark):
        df= spark.readStream \
                 .format("kafka") \
                 .option("kafka.bootstrap.servers", "broker:29092") \
                 .option("subscribe", topicName) \
                 .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load() 
        return df    

    def getDataColumns(self,sparkInputdf):
        nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss"
        jsonOptions = { "timestampFormat": nestTimestampFormat }
        dataDf = sparkInputdf \
            .select(from_json(col("value").cast("string"), self.eschema, jsonOptions).alias("data")) \
            .select("data.*") 
        return dataDf        
    
    def getRequiredColumns(self,df):
       rc = []
       for k,v in self.config['requiredcolumns'].items():
          if v in df.schema.names:
             rc.append(v)
             self.columnMapping[k]=v   
          elif k == 'usersession' and v not in df.schema.names:
             rc.append(self.config['requiredcolumns']['timestamp'])
             self.usetimestamp=True
             self.columnMapping[k]=self.config['requiredcolumns']['timestamp']
          else:
             raise Exception('Required column Mapping missing key:{} value:{}'.format(k,v))
                
       print(df.select([v for v in rc]).show()) 
       return df 
        
    def getPiiColumns(self,df):
       rc = []
       for k,v in self.config['piicolumns']['properties'].items():
          if v['type']=='object':
             for k1,v1 in v['properties'].items():   
                 if v1['sourcecol'] in df.schema.names:      
                     rc.append(v1['sourcecol'])
                     self.columnMapping[k1]=v1     
          else:
             if 'sourcecol' in v and v['sourcecol'] in df.schema.names:      
                 rc.append(v['sourcecol'])
                 self.columnMapping[k]=v
                    
       return df 
    
    def sessionId(self,df):
        print(self.columnMapping['timestamp'])
        df=df.withColumn(self.columnMapping['timestamp'], col(self.columnMapping['timestamp']).cast('timestamp'))
        #print(df.select(self.columnMapping['timestamp']).show())
        if self.usetimestamp == True:
            df=df.withColumn("usessionId", datemincol(df[self.columnMapping['timestamp']]))
        else:
            df=df.withColumn("usessionId", col(self.columnMapping['sessionid']))
        df=df.withColumn("partitioncol", datemincol(df[self.columnMapping['timestamp']]))    
        df=df.withColumn("year", date_format(col(self.columnMapping['timestamp']), "yyyy"))
        df=df.withColumn("month", date_format(df[self.columnMapping['timestamp']], "MM"))
        df=df.withColumn("day", date_format(df[self.columnMapping['timestamp']], "dd"))
        df=df.withColumn("hour", date_format(df[self.columnMapping['timestamp']], "HH"))
        df=df.withColumn("sessiontime", fn.to_timestamp(fn.concat(col('year'), \
                                                                  col('month'), \
                                                                  col("day"), \
                                                                  col("hour"), fn.lit(":00:00")), "yyyyMMddHH:mm:ss"))
        df=df.withColumn("part", partitioncol(col("partitioncol")))
        df=df.withColumn("userPart", partitioncol(col(self.columnMapping['userid'])))
        return df  
    
    def eventcol(self, df):
        df=df.withColumn('event_col', col(self.columnMapping['state']))
        return df
    
    def useridcol(self, df):
        df=df.withColumn('userid_col', col(self.columnMapping['userid']))
        return df

    def rowNum(self,df):
        w = Window.partitionBy(df.col(self.columnMapping['eventId'])).orderBy(df.col(self.columnMapping['timestamp']))
        df = df.withColumn('rnum',f.row_number().over(w))
        return df
    
    def selectLag(self,df):
        
        df=df.selectExpr("event_timestamp as date","usessionId as sessionId","page_url_path as event","user_domain_id as userId","userPart as userPart", \
                         "usessionId as sessionPart","event_timestamp as event_timestamp","event_type as eventtype")
        return df
    
    def writeBatch(self, df):
        #trigger(Trigger.ProcessingTime("100 seconds")). to add 
        df.writeStream.foreachBatch(raw_events_foreach_batch_function).start().awaitTermination()    
    
    def writeBatchJson(self, df):
        #trigger(Trigger.ProcessingTime("100 seconds")). to add 
        df.writeStream.foreachBatch(lambda batchdf,epochId: raw_events_foreach_batch_function_json(batchdf,epochId,self)).start().awaitTermination()    
    
    def writeStream(self,df):
        w = df.writeStream
        #.foreach(RawEventsTableForeachWriter()) #here we encrypt origcols, add noise to geo location, anonymize ip, user and other things \
        w.format("delta") \
         .partitionBy("year","month","day") \
         .trigger(Trigger.ProcessingTime("100 seconds")) \
         .outputMode("append") \
         .option("mergeSchema", "true") \
         .option("checkpointLocation", self.checkpointLocation) \
         .start(self.tableLocation) \
         .awaitTermination() 
        return

    def writeStreamToConsole(self,df):
        w = df.writeStream.trigger(once=True)   
        w.outputMode("append") \
         .format("json") \
         .option("path", self.location + self.eventName + "/test/") \
         .option("checkpointLocation", self.checkpointLocation) \
         .option("truncate", "false") \
         .start() \
         .awaitTermination() 
        return                                                    
    
        

if __name__ == '__main__':
    #create_topic()
    samplestring = '{"event_id": "a8fb35f6-dd4b-42bb-95a6-ef2af50530a6", "event_timestamp": "2021-07-04 06:02:10.443611", "event_type": "pageview", "page_url": "http://www.dummywebsite.com/product_a", "page_url_path": "/product_a", "referer_url": "www.instagram.com", "referer_url_scheme": "http", "referer_url_port": "80", "referer_medium": "internal", "utm_medium": "organic", "utm_source": "instagram", "utm_content": "ad_2", "utm_campaign": "campaign_2", "click_id": "c4d7416b-8404-4bc4-8885-a81674fc5df6", "geo_latitude": "33.4425", "geo_longitude": "129.96972", "geo_country": "JP", "geo_timezone": "Asia/Tokyo", "geo_region_name": "Karatsu", "ip_address": "60.117.126.41", "browser_name": "Opera", "browser_user_agent": "Opera/9.78.(X11; Linux x86_64; ti-ET) Presto/2.9.179 Version/11.00", "browser_language": "dz_BT", "os": "Android 2.3", "os_name": "Android", "os_timezone": "Asia/Tokyo", "device_type": "Mobile", "device_is_mobile": true, "user_custom_id": "waltertaylor@yahoo.com", "user_domain_id": "78c05cb7-649a-46ae-af67-88966e524fe9"}'
    spark = session()
    df = spark.read.json(spark.sparkContext.parallelize([samplestring]))
    print(df.schema.json())
    eventsTable = RawEventsTable("/opt/workspace/","clickstream",getSparkSchemaFromJson(df.schema.json()))
    #eventsTable.createStateTable(spark)
    df= eventsTable.getRequiredColumns(df)
    eventsTable.getPiiColumns(df)

    #read stream
    dfstream = eventsTable.readStream(spark)
    dfstream=eventsTable.getDataColumns(dfstream)
    print(dfstream.schema.names)
    dfstream=eventsTable.sessionId(dfstream)
    dfstream=eventsTable.eventcol(dfstream)
    dfstream=eventsTable.eventcol(dfstream)
    print(dfstream.schema.names)
    eventsTable.writeBatchJson(dfstream)
    
    
    #dfstream=eventsTable.selectLag(dfstream)
    #eventsTable.writeStreamToConsole(dfstream)
    #eventsTable.writeStreamToConsole(dfstream)
    #dfstream=eventsTable.sessionId(dfstream)
    #dfstream=eventsTable.eventcol(dfstream)
    #dfstream=eventsTable.useridcol(dfstream)
    
    #deltaTable = DeltaTable.forPath(spark, eventsTable.tableLocation)
    #deltaTable.toDF().show()
    