from kafka import KafkaProducer
import json
import time
import random

# Создаем экземпляр KafkaProducer, который подключается к локальному брокеру Kafka.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',

    # Эта функция преобразует объект Python в JSON-строку и затем кодирует её в байты UTF-8, чтобы Kafka мог отправить данные в бинарном виде.
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

users = ["alice", "bob", "carol", "dave"]

while True:
    data = {
        "user": random.choice(users),
        "event": "login",
        "timestamp": time.time()
    }
    producer.send("user_events", value=data)
    print("Sent:", data)
    time.sleep(1)