import json
import secrets

import redis
import sentry_sdk
from decouple import config
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search
from flask import Flask, abort, redirect, render_template, request, url_for
from kafka import KafkaProducer
from sentry_sdk.integrations.flask import FlaskIntegration

FLASK_DEBUG = config("FLASK_DEBUG", default=False, cast=bool)
SENTRY_DNS = config("SENTRY_DNS", default=None, cast=str)

FETCH_TOPIC_NAME = 'webpage_fetch'
BOOTSTRAP_SERVERS = config('BOOTSTRAP_SERVERS',
                           default='kafka:9092',
                           cast=lambda v: [s.strip() for s in v.split(',')])

# Log the errors to sentry
if SENTRY_DNS and not FLASK_DEBUG:
    # In production mode, track all errors with sentry.
    sentry_sdk.init(
        dsn=SENTRY_DNS,
        integrations=[FlaskIntegration()]
    )

r = redis.Redis(host='hd_redis', port=6379)
es = Elasticsearch(['hd_es'], port=9200)

app = Flask(__name__)


def generate_token():
    return secrets.token_urlsafe(20)


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


def publish_message(producer_instance, topic_name, data):
    """ Excepts data to be a dictionary """
    try:
        if not isinstance(data, dict):
            raise ValueError
        producer_instance.send(topic_name, value=data)  # Send the message to the queue
        producer_instance.flush()
    except ValueError as e:
        print('Error, data is expected to be a dictionary!', flush=True)
    except Exception as e:
        print(f'Unexpected error in publishing message. {str(e)}', flush=True)


def request_webpage_fetch(url, token):
    """ Orders fetching the webpage to spider microservice.

    Inserts messages to kafka queue. These messages will pulled by spider
    microservice later on and processed.
    """
    kafka_producer = connect_kafka_producer()
    data = {'url': url, 'token': token}

    try:
        page_data = {'url': url, 'reviewed': False}
        r.mset({token: json.dumps(page_data)})
    except:
        print("Couldn't connect to Redis.", flush=True)
    publish_message(kafka_producer, FETCH_TOPIC_NAME, data)

    print(f'Requested to fetch {url}.')

    if kafka_producer is not None:
        kafka_producer.close()  # Close the connection


def mark_as_reviewed(token):
    ''' Marks the webpage as reviewed and ready to show to the user. '''
    page_data = json.loads(r.get(token))
    page_data['reviewed'] = True
    r.mset({token: json.dumps(page_data)})


@app.context_processor
def inject_debug():
    """ Adds debug information for every template. """
    return dict(debug=FLASK_DEBUG)


@app.errorhandler(404)
def not_found(e):
    return render_template("error.html", header="404 Not Found", message=str(e)), 404


@app.errorhandler(500)
def server_error(e):
    return render_template("error.html", header="500 Internal server problem", message=str(e)), 500


@app.route('/')
def index():
    return render_template("index.html")


@app.route('/fetch')
def fetch():
    token = request.args.get('token', type=str)
    if not token:
        abort(404)

    # Check if manager already fetched the webpage and it has been reviewed
    try:
        page_data = json.loads(r.get(token))
        url = page_data['url']
        reviewed = page_data['reviewed']
    except Exception as e:
        reviewed = False
        print("Error while accessing redis.", flush=True)

    if reviewed:
        return redirect(url_for('check', url=url))
    return render_template("fetch_in_progress.html")


@app.route('/check')
def check():
    url = request.args.get('url', type=str)
    if not url:
        abort(404)

    page_info = None
    # Check if record is in redis or in Elasticsearch:
    try:
        page_info = r.get(url)
        if page_info is not None:
            print("Webpage in Redis.", flush=True)
    except Exception as e:
        print("Error while accessing redis.", flush=True)

    if page_info is None:
        try:
            s = Search(index="report").using(es).query("match", url=url)
            res = s.execute()
            print("No. of total hits: " + str(res.hits.total.value), flush=True)
            if res.hits.total.value > 0:
                report = res.to_dict()["hits"]["hits"][0]["_source"]["report"]
                page_info = {url: report}
        except NotFoundError as e:
            print("Index does not exist yet.", flush=True)

    if page_info is not None:
        return render_template("check.html", page_info=page_info)

    # Request spider to fetch the webpage
    token = generate_token()
    request_webpage_fetch(url, token)

    return redirect(url_for('fetch', token=token))


@app.route('/api/v1/reviewed')
def api_reviewed():
    token = request.args.get('token', type=str)
    if not token:
        abort(404)

    mark_as_reviewed(token)

    return token, 200


if __name__ == '__main__':
    app.run(debug=FLASK_DEBUG, host='0.0.0.0', port=8000)
