from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
	'henry-topic2',        # topic name
	bootstrap_servers = [    #MSK Cluster Bootstrap Private Endpoint address
		'b-2.henrymskcluster.jm4797.c4.kafka.ap-northeast-2.amazonaws.com:9092',
		'b-1.henrymskcluster.jm4797.c4.kafka.ap-northeast-2.amazonaws.com:9092'
	],
    auto_offset_reset='latest',
    group_id='mygroup',
    enable_auto_commit=True,
    value_deserializer=lambda x : loads(x.decode('utf-8')),
    consumer_timeout_ms=10000
)

for msg in consumer:
    print(f'topic={msg.topic}, partition={msg.partition}, offset={msg.offset}, key={msg.key}, value={msg.value}')