from kafka import KafkaConsumer
import json
import clickhouse_connect

# Cоздаёт объект Kafka-потребителя, который подключается к Kafka-брокеру по адресу localhost:9092
# и подписывается на топик user_events.

# Параметр auto_offset_reset='earliest' означает, что при отсутствии сохранённых
# смещений (offset'ов) потребитель начнёт чтение с самого начала топика.

# enable_auto_commit=True включает автоматическую фиксацию (commit) смещений, что позволяет Kafka знать,
# какие сообщения уже были обработаны.

# value_deserializer=lambda x: json.loads(x.decode('utf-8')) указывает, как преобразовать полученное сообщение
# из байтового формата: сначала байты декодируются в строку, затем строка парсится как JSON, превращаясь в словарь
# Python. Таким образом, этот потребитель читает сообщения в формате JSON из указанного топика, начиная с начала,
# и автоматически отслеживает, какие сообщения уже прочитаны.

consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id="user-logins-consumer"
)

client = clickhouse_connect.get_client(host='localhost', port=8123, username='user', password='strongpassword')

client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
""")

for message in consumer:
    data = message.value
    print("Received:", data)

    # client.command(
    #    f"INSERT INTO user_logins (username, event_type, event_time) VALUES ('{data['user']}', '{data['event']}', toDateTime({data['timestamp']}))"
    # )

    client.insert(
        'user_logins',
        [{
            'username': data['user'],
            'event_type': data['event'],
            'event_time': data['timestamp']
        }]
    )
