from consumer import KafkaConsumerClass
from producer import KafkaProducerClass
from confluent_kafka.admin import AdminClient, NewTopic

def consuming(topicList: list, consumer: KafkaConsumerClass):
    try:
        consumer.consumer.subscribe(topicList)

        while True:
            msg = consumer.consumer.poll(timeout=60.0)
            if msg is None: continue

            if msg.error():
                print("error: ", msg.error())
            else:
                print("message: ", msg.value())
    finally:
        consumer.consumer.close()

def list_topics():
    admin_client = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})

    # Retrieve metadata for all topics
    metadata = admin_client.list_topics(timeout=10)

    # Extract topic names from the metadata
    topic_names = metadata.topics.keys()

    # Print the list of topics
    print("List of topics:", topic_names)


if __name__ == '__main__':
    topic = "test-topic"
    producer = KafkaProducerClass()
    producer.producer.produce(topic, key="key", value="123456789", callback=producer.callback)
    # producer.producer.poll(1)
    # list_topics()
    consumer = KafkaConsumerClass()
    consuming([topic], consumer=consumer)


