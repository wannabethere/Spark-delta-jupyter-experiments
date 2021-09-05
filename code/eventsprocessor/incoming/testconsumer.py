#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/11 21:06
# @Author  : MengnanChen
# @Site    : 
# @File    : producer.py
# @Software: PyCharm Community Edition

'''
produce data into kafka
'''

from pykafka import KafkaClient
from confluent_kafka.admin import AdminClient, NewTopic
from fake_web_events import Simulation
import json
import random
import threading
import time


'''
global variable
'''

data_produce_duration=100 # data production duration(second)

kafka_topic='test' # kafka topic
hosts = 'broker:29092'
def create_topic():
    admin_client = AdminClient({
        "bootstrap.servers": hosts
    })

    topic_list = []
    topic_list.append(NewTopic(kafka_topic, 1, 1))
    admin_client.create_topics(topic_list)



def consumer_data():  
    client = KafkaClient(hosts=hosts)
    print('client topics:', client.topics)
    topic = client.topics[bytes(kafka_topic, encoding='utf-8')]
    consumer = topic.get_simple_consumer()
    for message in consumer:
        if message is not None:
         print(message.value)

        
#df.withColumn("timestamp_prev", lag("timestamp").over(Window.partitionBy("id").orderBy("timestamp")))        
#val w = org.apache.spark.sql.expressions.Window.orderBy("date")  

#import org.apache.spark.sql.functions.lag
#val leadDf = df.withColumn("new_col", lag("volume", 1, 0).over(w))
#leadDf.show()


if __name__ == '__main__':
    #create_topic()
    print("Running consumer")
    #consumer_data()
