from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json

import main

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
producer_config = dict(config_parser['kafka_client'])
consumer_config = dict(config_parser['kafka_client'])
consumer_config.update(config_parser['consumer'])
prediction_producer = Producer(producer_config)


def start_service():
    prediction_consumer = Consumer(consumer_config)
    prediction_consumer.subscribe(['data-preprocessing'])
    while True:
        msg = prediction_consumer.poll(0.1)
        if msg is None:
            #print("wait...")
            pass
        elif msg.error():
            pass
        else:
            data = json.loads(msg.value())
            print(f'Tipo: {type(data)}')
            prediction = main.predict_dt(data)
            print(f'Prediccion: {prediction}')
            prediction_producer.produce('data-prediction', value=json.dumps(prediction))

