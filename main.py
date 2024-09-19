from confluent_kafka import Producer, Consumer, KafkaException


# Making the producer which connects with the broker and sends the produced data
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

# Making consumer which consumes the data which is received by the broker
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stream-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['input_topic'])

def process_message(message):
    return message.upper()

try:
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        input_message = msg.value().decode('utf-8')
        processed_message = process_message(input_message)
        producer.produce('output_topic', key=msg.key(), value=processed_message)
        producer.flush()
        print(f"Produced Message: {processed_message}")
except KeyboardInterrupt:
    raise "Streaming Stopped Manually by the User"
finally:
    consumer.close()

