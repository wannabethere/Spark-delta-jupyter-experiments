import json
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

# Restore schema from json:
def getSparkSchemaFromJson(schema_json):
    new_schema = StructType.fromJson(json.loads(schema_json))
    return new_schema

def get_merged_schema(delta_table_schema, json_data_schema):    
    print('str(len(delta_table_schema.fields)) -> ' + str(len(delta_table_schema.fields)))
    print('str(len(json_data_schema.fields)) -> '+ str(len(json_data_schema.fields)))
    no_commom_elements=False
    no_new_elements=False
    import numpy as np
    struct_field_array=[]
    if len(set(delta_table_schema.names).intersection(set(json_data_schema.names))) > 0:
        common_col=set(delta_table_schema.names).intersection(set(json_data_schema.names))
        print('common_col len: -> '+ str(len(common_col)))
        for name in common_col:
            for f in delta_table_schema.fields:
                if(f.name == name):
                    struct_field_array.append(StructField(f.name, f.dataType, f.nullable))
    else:
        no_commom_elements=True
        print("no common elements")

    if len(np.setdiff1d(json_data_schema.names,delta_table_schema.names)) > 0:
        diff_list = np.setdiff1d(json_data_schema.names,delta_table_schema.names)
        print('diff_list len: -> '+ str(len(diff_list)))
        for name in diff_list:
            for f in json_data_schema.fields:
                if(f.name == name):
                    struct_field_array.append(StructField(f.name, f.dataType, f.nullable))
    else:
        no_new_elements=True
        print("no new elements")
      
    print('len(StructType(struct_field_array)) -> '+str(len(StructType(struct_field_array))))
    df=spark.createDataFrame(spark.sparkContext.emptyRDD(),StructType(struct_field_array))
    if no_commom_elements and no_new_elements: 
        return StructType(None) 
    else: 
        return df.select(sorted(df.columns)).schema

def str_to_bool(value):
    FALSE_VALUES = ['false', 'no', '0']
    TRUE_VALUES = ['true', 'yes', '1']
    lvalue = str(value).lower()
    if lvalue in (FALSE_VALUES): return False
    if lvalue in (TRUE_VALUES):  return True
        raise Exception("String value should be one of {}, but got '{}'.".format(FALSE_VALUES + TRUE_VALUES, value))


def validate_required_argument_and_return_value(name):
    value = getArgument(name)
    if len(value) < 1:
        dbutils.notebook.exit("'{}' argument value is required.".format(name))
    return value

def infer_topic_schema_json(topic):
    df_json = (spark.read
               .format("kafka") \
               .option("kafka.bootstrap.servers", kafka_broker) \
               .option("subscribe", topic) \
               .option("startingOffsets", "earliest") \
               .option("endingOffsets", "latest") \
               .option("failOnDataLoss", "false") \
               .load() \
               # filter out empty values
               .withColumn("value", expr("string(value)")) \
               .filter(col("value").isNotNull()) \
               # get latest version of each record
               .select("key", expr("struct(offset, value) r")) \
               .groupBy("key").agg(expr("max(r) r")) \
               .select("r.value"))
    
    # decode the json values
    df_read = spark.read.json(
    df_json.rdd.map(lambda x: x.value), multiLine=True)
    
    # drop corrupt records
    if "_corrupt_record" in df_read.columns:
        df_read = (df_read.filter(col("_corrupt_record").isNotNull()).drop("_corrupt_record"))
    return df_read.schema.json()