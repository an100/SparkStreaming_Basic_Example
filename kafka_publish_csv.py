
import os,sys,re,csv
import datetime,time
import random
from kafka import KafkaProducer

try:
    number_of_records = int(sys.argv[1])
except:
    number_of_records = 10

brokers     = ['sandbox.hortonworks.com:6667']
topic       = 'dztopic1'

#producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer = KafkaProducer(bootstrap_servers=brokers)

for i in range(number_of_records):
    row = [str(random.randint(1000000,9999999)), str(random.randint(0,1)), "10", "20", str(random.uniform(10.0, 90.0))]
    record = '|'.join(row)
    print str(record)
    producer.send(topic, record)
    time.sleep(1)

#ZEND
