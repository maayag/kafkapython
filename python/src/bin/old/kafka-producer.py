#!/usr/bin/env python3

#Usage:
# pip install confluent-python
# ./producer.py <my-topic> <my-key> <my-message>

# See:
# - https://raw.githubusercontent.com/simplesteph/kafka-stack-docker-compose/master/zk-multiple-kafka-multiple.yml
# - https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05

import sys
from confluent_kafka import Producer

def main(args): 
    try:
        topic = args[0]
        key = args[1]
        message = args[2]
    except Exception as ex:
        print("Failed to set topic, key, or message")

    producer = get_kafka_producer()
    publish(producer, topic, key, message)

def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))


def publish(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.produce('test-topic2', value_bytes, callback=lambda err, original_msg=value_bytes: self.delivery_report(
                err, original_msg
            ),)
        producer_instance.flush()
        print(f"Publish Successful ({key}, {value}) -> {topic_name}")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def get_kafka_producer():
    _producer = None
    producer_config = {  
            'bootstrap.servers': '192.168.123.29092', 
            'socket.timeout.ms' : 100,
            'api.version.request': 'false',
            'broker.version.fallback': '0.3.3'}

    try:
        _producer = Producer( {  
            'bootstrap.servers': '192.168.123:9092', 
            'socket.timeout.ms' : 100,
            'api.version.request': 'false',
            'broker.version.fallback': '0.9.0'})

    except Exception as exp:
        print('Exception while connecting Kafka')
        print(str(exp))
    finally:
        return _producer


if __name__ == "__main__":
    main(sys.argv[1:])


