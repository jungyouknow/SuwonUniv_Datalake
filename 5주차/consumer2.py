from confluent_kafka import Consumer 
import json 
import time 

consumer = Consumer({ 'bootstrap.servers': 'b-2.henrymskcluster.4ka9rz.c4.kafka.ap-northeast-2.amazonaws.com:9092, b-1.henrymskcluster.4ka9rz.c4.kafka.ap-northeast-2.amazonaws.com:9092', 
                      'group.id': 'mygroup', 'auto.offset.reset': 'smallest'})

consumer.subscribe(['henry-topic3']) 

while True: 
   msg = consumer.poll(1.0) 
   if msg is None: 
      continue 
   if msg.error(): 
      print("Consumer error: {}".format(msg.error())) 
      continue 
   print('Received message: {}'.format(json.loads(msg.value()))) 
   time.sleep(1)

consumer.close()