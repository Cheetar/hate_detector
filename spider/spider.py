import json
from datetime import datetime

import requests

from decouple import config
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer

es = Elasticsearch(['hd_es'], port=9200)

FETCH_TOPIC_NAME = 'webpage_fetch'
REVIEW_TOPIC_NAME = 'webpage_review'
BOOTSTRAP_SERVERS = config('BOOTSTRAP_SERVERS',
                           default='kafka:9092',
                           cast=lambda v: [s.strip() for s in v.split(',')])


def publish_message(producer_instance, topic_name, data):
    """ Excepts data to be a dictionary """
    try:
        if not isinstance(data, dict):
            raise ValueError
        producer_instance.send(topic_name, value=data)  # Send the message to the queue
        producer_instance.flush()
    except ValueError as e:
        print('Error, data is expected to be a dictionary!')
    except Exception as e:
        print(f'Unexpected error in publishing message. {str(e)}')


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                  api_version=(0, 10),
                                  value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    except Exception as e:
        print(f'Unexpected error while connecting to Kafka. {str(e)}')
    finally:
        return _producer


def request_webpage_review(url, token, webpage_html):
    """ Orders reviewing the url to reviewer microservice.

    Inserts messages to kafka queue. These messages will pulled by reviewer
    microservice later on and processed.
    """
    kafka_producer = connect_kafka_producer()
    data = {'url': url, 'token': token, 'html': webpage_html}

    publish_message(kafka_producer, REVIEW_TOPIC_NAME, data)

    print(f'Request sent to review {url}.')

    if kafka_producer is not None:
        kafka_producer.close()  # Close the connection


def save_to_es(url, token, webpage_html):
    # Save the bare html to Elasticsearch
    doc = {
        'url': url,
        'html': webpage_html,
        'timestamp': datetime.now(),
    }
    res = es.index(index="webpage", doc_type='webpage', body=doc)
    es.indices.refresh(index="webpage")


def fetch_webpage(url, token):
    print("Fetching webpage {}...".format(url))

    webpage_html = requests.get(url).text

    save_to_es(url, token, webpage_html)
    request_webpage_review(url, token, webpage_html)


def consume_message(data):
    if not isinstance(data, dict) or 'url' not in data or 'token' not in data:
        raise ValueError('Invalid message format!')

    # try:
    fetch_webpage(data['url'], data['token'])
    # except Exception as e:
    #    print(f"Unexpected error when fetching a webpage. {str(e)}")


if __name__ == '__main__':
    print('Spider starting..')

    consumer = KafkaConsumer(FETCH_TOPIC_NAME,
                             auto_offset_reset='latest',
                             bootstrap_servers=BOOTSTRAP_SERVERS,
                             api_version=(0, 10),
                             consumer_timeout_ms=1000,
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    consumer.subscribe([FETCH_TOPIC_NAME])  # Poll messages from the topic

    try:
        while True:
            # Response format is {TopicPartiton('topic1', 1): [msg1, msg2]}
            msg_pack = consumer.poll(timeout_ms=1000,  # Wait for 1s when no data in buffer
                                     max_records=1)  # Poll maximum 1 record at a time

            for tp, messages in msg_pack.items():
                for message in messages:
                    # try:
                    consume_message(message.value)
                    # except Exception as e:
                    #    print(f'Error while consuming the message. {str(e)}')
    finally:
        consumer.close()
