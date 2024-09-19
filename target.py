from main import consumer_config
from confluent_kafka import Consumer

consumer = Consumer(consumer_config)
consumer.subscribe(['output_topic'])

try:
    while True:
        msg = consumer.poll(5.0)
        data = msg.value().decode('utf-8')
        print(data)
except KeyboardInterrupt:
    raise "Streaming Output Interrupted by User"
finally:
    consumer.close()
