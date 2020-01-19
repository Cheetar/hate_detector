import json
from datetime import datetime

import requests
from hatesonar import Sonar

import redis
from decouple import config
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer

APP_API = "http://app:8000/api/v1/reviewed"

REVIEW_TOPIC_NAME = 'webpage_review'
BOOTSTRAP_SERVERS = config('BOOTSTRAP_SERVERS',
                           default='kafka:9092',
                           cast=lambda v: [s.strip() for s in v.split(',')])

r = redis.Redis(host='hd_redis', port=6379)
es = Elasticsearch(['hd_es'], port=9200)


def save_to_redis(url, report):
    r.mset({url: json.dumps(report)})


def save_to_es(url, report):
    # Save the report to es
    doc = {
        'url': url,
        'report': report,
        'timestamp': datetime.now(),
    }
    res = es.index(index="report", doc_type='report', body=doc)
    es.indices.refresh(index="report")


def get_report(webpage_html):
    sonar = Sonar()
    report = sonar.ping(text=webpage_html)
    return report


def inform_app(token):
    ''' Informs app that the webpage have been reviewed. '''
    requests.get(APP_API, {'token': token})


def review_webpage(url, webpage_html):
    print("Reviewing webpage {}...".format(url))

    report = get_report(webpage_html)
    save_to_es(url, report)
    save_to_redis(url, report)


def consume_message(data):
    if not isinstance(data, dict) or 'url' not in data or 'html' not in data or 'token' not in data:
        raise ValueError('Invalid message format!')

    try:
        review_webpage(data['url'], data['html'])
        inform_app(data['token'])
    except Exception as e:
        print(f"Unexpected error when fetching a webpage. {str(e)}")


if __name__ == '__main__':
    print('Reviewer starting..')

    consumer = KafkaConsumer(REVIEW_TOPIC_NAME,
                             auto_offset_reset='latest',
                             bootstrap_servers=BOOTSTRAP_SERVERS,
                             api_version=(0, 10),
                             consumer_timeout_ms=1000,
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    consumer.subscribe([REVIEW_TOPIC_NAME])  # Poll messages from the topic

    try:
        while True:
            # Response format is {TopicPartiton('topic1', 1): [msg1, msg2]}
            msg_pack = consumer.poll(timeout_ms=1000,  # Wait for 1s when no data in buffer
                                     max_records=1)  # Poll maximum 1 record at a time

            for tp, messages in msg_pack.items():
                for message in messages:
                    try:
                        consume_message(message.value)
                    except Exception as e:
                        print(f'Error while consuming the message. {str(e)}')
    finally:
        consumer.close()
