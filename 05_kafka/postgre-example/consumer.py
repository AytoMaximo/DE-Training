from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    "user_events",  # название топика Kafka, из которого читаются сообщения
    bootstrap_servers="localhost:9092",  # адрес и порт сервера Kafka
    auto_offset_reset='earliest',  # начинать чтение с самого раннего доступного сообщения, если нет смещений
    enable_auto_commit=True,  # автоматически сохранять смещения после обработки сообщений
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # функция для преобразования байтовых сообщений в объекты Python (десериализация JSON)
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP
)
""")
conn.commit()

for message in consumer:
    data = message.value
    print("Received:", data)

    cursor.execute(
        "INSERT INTO user_logins (username, event_type, event_time) VALUES (%s, %s, to_timestamp(%s))",
        (data["user"], data["event"], data["timestamp"])
    )
    conn.commit()