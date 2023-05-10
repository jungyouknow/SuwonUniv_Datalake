import json
import requests
from confluent_kafka import Producer
import time

def delivery_report(err, msg):
    """ 
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush(). 
    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
  
  
producer = Producer({'bootstrap.servers': 'b-2.henrymskcluster.4ka9rz.c4.kafka.ap-northeast-2.amazonaws.com:9092, b-1.henrymskcluster.4ka9rz.c4.kafka.ap-northeast-2.amazonaws.com:9092'})
    
while True:
    api_server='http://3.35.219.68:5000/api'            # API Server Public IPv4
    request=requests.get(api_server)
    data=json.loads(request.content)
    json_data = json.dumps(data)
    producer.poll(0)
    producer.produce("henry-topic3", json_data, callback=delivery_report)               # topic name
    producer.flush()
    time.sleep(1)