from time import sleep
from json import dumps
from kafka import KafkaProducer
import csv
import logging


def connect_kafka():
    for count in range(3):
        try:
            my_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8')
            )
            return my_producer
        except Exception as error:
            logging.exception(
                f"Error occurred while connecting to Kafka, error is {error}")


my_producer = connect_kafka()
with open("Batch.csv", mode='r') as file:
    csv_file = csv.DictReader(file)
    for row in csv_file:
        my_producer.send('testnum', value=row)
