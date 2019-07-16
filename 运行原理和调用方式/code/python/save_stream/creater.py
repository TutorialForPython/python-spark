import time
from secrets import token_hex
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
    key = token_hex(1).encode("utf-8")
    value = token_hex(5).encode("utf-8")
    print(f"send{key}:{value}")
    future = producer.send('topic1', key= key, value= value)
    #result = future.get(timeout= 10)
    time.sleep(1)