from confluent_kafka import Producer
import socket

class KafkaProducerClass:
    def __init__(self):
        self.producer = self.create_producer()

    @staticmethod
    def callback(err, msg):
        if err is not None:
            print("Failed to deliver: %s", str(err))
        else:
            print("Delivery successful: %s", str(msg))

    def create_producer(self):
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
        })
        print("producer: ", producer)
        return producer