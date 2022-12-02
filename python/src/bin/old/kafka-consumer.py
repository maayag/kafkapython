#!/usr/bin/env python3

import sys
from confluent_kafka import Consumer

# Usage:
# pip install kafka-python
# ./consumer.py <my-topic>

# NOTE: only runs for 10 seconds as per consumer_timeout_ms

# See:
# - https://raw.githubusercontent.com/simplesteph/kafka-stack-docker-compose/master/zk-multiple-kafka-multiple.yml
# - https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05

def main(args):
    try:
        topic = args[0]
    except Exception as exp:
        print("Failed to set topic")

    consumer = get_kafka_consumer(topic)
    subscribe(consumer)

def subscribe(consumer_instance):
    try:
        for event in consumer_instance:
            key = event.key.decode("utf-8")
            value = event.value.decode("utf-8")
            print(f"Message Received: ({key}, {value}")
        consumer_instance.close()
    except Exception as exp:
        print("Exception in subscribing " + str(exp))
    
def get_kafka_consumer(topic_name, group_id, servers=['192.168.1.123:9092']):
    _consumer = None
    consumer_config = {
        'bootstrap.servers' : servers,
        'group.id' : group_id,
        'auto.offset.reset': 'largest',
        'enable.auto.commit': 'false',
        'max.poll.interval.ms': '86400000'
    }

    try:
        _consumer = Consumer(consumer_config)
        _consumer.subscribe([topic_name])
    except Exception as exp:
        print("Exception while connecting Kafka " + str(exp))
    finally:
        return _consumer

if __name__ == "__main__":
    main(sys.argv[1:])


