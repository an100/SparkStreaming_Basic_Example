
import os,sys,re,csv
import datetime,time
import random
from kafka import KafkaProducer

data_file   = '/tmp/mosaic_data.csv' 
brokers     = ['sandbox.hortonworks.com:6667']
topic       = 'dztopic1'

file    = csv.reader(open(data_file, 'rb'))
header  = file.next()

#producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer = KafkaProducer(bootstrap_servers=brokers)

for row in file:
    time.sleep(4)
    record = '|'.join(row)
    print str(record)
    producer.send(topic, record)

#ZEND
