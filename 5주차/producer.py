from kafka import KafkaProducer
from json import dumps
import time

producer = KafkaProducer(
	acks=0,
	bootstrap_servers = [
		'b-2.henrymskcluster.jm4797.c4.kafka.ap-northeast-2.amazonaws.com:9092',
		'b-1.henrymskcluster.jm4797.c4.kafka.ap-northeast-2.amazonaws.com:9092'
	],
	value_serializer=lambda x : dumps(x).encode('utf-8')
)
topic = 'henry-topic'
start = time.time()
for i in range(20) : 
	data = {'num' : str(i)}
	producer.send(topic, value=data)
	producer.flush()
	print('sended data : ', data)
	time.sleep(3)
	
print('elpased : ', time.time() - start)