from kafka import KafkaProducer
import json

def create_kafka_producer(bootstrap_servers, client_id=None, acks='all', retries=5, linger_ms=0):
    """
    Create and return a Kafka producer with the specified configuration.

    :param bootstrap_servers: List of Kafka broker addresses.
    :param client_id: Optional client identifier.
    :param acks: Acknowledgment level ('all', '1', '0').
    :param retries: Number of retries for sending messages.
    :param linger_ms: Time to wait before sending messages (in milliseconds).
    :return: Configured KafkaProducer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        acks=acks,
        retries=retries,
        linger_ms=linger_ms,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

if __name__ == "__main__":
    producer = create_kafka_producer(
        bootstrap_servers=['localhost:9092'],
        client_id='my-producer',
        acks='all',
        retries=3,
        linger_ms=10
    )
    # Send a test message
    future = producer.send('orders', value={"order_id": 123, "amount": 250.75, "items": ["item1", "item2"], "user_id": 456})
    result = future.get(timeout=10)
    print(f'Message sent to partition {result.partition} with offset {result.offset}')
    producer.flush()
