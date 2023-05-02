FROM      	python:3

ENV        	PYTHONUNBUFFERED 1
RUN        	mkdir /kafka-client
WORKDIR	/kafka-client
COPY			requirements.txt /kafka-client/
RUN			pip install -r requirements.txt
COPY			producer.py  /kafka-client/producer.py
CMD			["python", "./producer.py"]