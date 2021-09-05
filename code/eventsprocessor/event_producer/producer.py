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
from hashids import Hashids
import base64
import binascii

'''
global variable
'''

data_produce_duration=1000 # data production duration(second)

kafka_topic='test' # kafka topic
hosts = 'localhost:9092'
def create_topic():
    admin_client = AdminClient({
        "bootstrap.servers": hosts
    })

    topic_list = []
    topic_list.append(NewTopic(kafka_topic, 1, 1))
    admin_client.create_topics(topic_list)



def produce_data():  
    client = KafkaClient(hosts=hosts)
    print('client topics:', client.topics)
    topic = client.topics[bytes(kafka_topic, encoding='utf-8')]
    producer = topic.get_producer()
    simulation = Simulation(user_pool_size=100, sessions_per_day=2000)
    events = simulation.run(duration_seconds=data_produce_duration)        
    for event in events:
        h = binascii.hexlify(event['user_custom_id'].encode('utf-8'))
        event['user_custom_id']=  int(h, 16)
        producer.produce(bytes(json.dumps(event), encoding='utf-8'))
           

    

if __name__ == '__main__':
    #create_topic()
    produce_data()
