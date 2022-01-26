
import random
import time as tm
from kafka import KafkaProducer
from json import dumps


topics = "test"

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              api_version=(0,11,5),
              value_serializer=lambda x: dumps(x).encode('utf-8'))

options = ['big', 'small', 'medium', 'no-size']

res = random.choice(options) 

producer.send(topics, value =  res)
producer.flush()
tm.sleep(2)
print(res)
