from kafka import KafkaConsumer
import json

def create_kafka_consumer(bootstrap_servers, topic, group_id=None, auto_offset_reset='earliest', enable_auto_commit=True):
    """
    Create and return a Kafka consumer with the specified configuration.

    :param bootstrap_servers: List of Kafka broker addresses.
    :param topic: Topic to subscribe to.
    :param group_id: Optional consumer group identifier.
    :param auto_offset_reset: Offset reset policy ('earliest', 'latest', 'none').
    :param enable_auto_commit: Whether to enable automatic offset committing.
    :return: Configured KafkaConsumer instance.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    return consumer

if __name__ == "__main__":
    consumer = create_kafka_consumer(
        bootstrap_servers=['localhost:9092'],
        topic='orders',
        group_id='my-consumer-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    # Poll for messages
    try:
        for message in consumer:
            print(f'Received message: {message.value} from partition {message.partition} with offset {message.offset}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
