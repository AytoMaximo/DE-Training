import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

#                                            преобразует время в формат UNIX timestamp (число секунд от 1970-01-01)
cursor.execute("SELECT username, event_type, extract(epoch FROM event_time) FROM user_logins")
rows = cursor.fetchall()

for row in rows:
    data = {
        "user": row[0],
        "event": row[1],
        "timestamp": float(row[2])  # преобразуем Decimal → float
    }
    producer.send("user_events", value=data)
    print("Sent:", data)
    time.sleep(0.5)

# В PostgreSQL функция extract(epoch FROM ...) возвращает количество секунд
# с плавающей точкой (тип double precision) с 1970-01-01.

# Однако драйвер psycopg2 может возвращать это значение как Decimal для точности при работе с числами с плавающей точкой.
# Преобразование из Decimal в float нужно, чтобы сериализовать данные в JSON
# (стандартный сериализатор не поддерживает Decimal) и для совместимости с другими системами, ожидающими числовой тип float.

# Таким образом, timestamp может быть Decimal из-за особенностей работы драйвера с типами данных PostgreSQL.