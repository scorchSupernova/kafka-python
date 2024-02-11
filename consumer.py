from confluent_kafka import Consumer
class KafkaConsumerClass:
    def __init__(self):
        self.consumer = self.create_consumer()

    def create_consumer(self):
        consumer = Consumer({
            'bootstrap.servers': '127.0.0.1:9092',
            'group.id': 'None',
            'auto.offset.reset': 'smallest'
        })
        print("consumer: ", consumer)
        return consumer




