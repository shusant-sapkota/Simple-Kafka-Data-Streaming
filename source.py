# from main import producer_config
from confluent_kafka import Producer
import time

print("Hello")

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)
i = 1

while True:
    data = f'Hello ---> {i}'
    #print(data)
    producer.produce('input_topic', key=str(i).encode('utf-8'), value=data)
    print(data)
    i = i + 1
    time.sleep(4)