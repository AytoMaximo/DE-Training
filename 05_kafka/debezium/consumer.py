from kafka import KafkaConsumer
import json

#  Сначала скрипт создаёт Kafka-консумер, который подключается к брокеру Kafka по адресу localhost:29092
#  и подписывается на топик pgserver1.public.my_table. Этот топик автоматически создаётся Debezium
#  и содержит информацию обо всех изменениях в таблице: новых строках, обновлениях и удалениях.

consumer = KafkaConsumer(
    'pgserver1.public.my_table',  # Название топика Debezium
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='pg-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Затем скрипт запускает бесконечный цикл, в котором каждое новое сообщение из Kafka обрабатывается.
# Если операция — это вставка ("op": "c"), он выводит ID, имя и дату создания новой строки.
# Если пришло обновление или удаление, скрипт печатает это событие целиком.

print("🟢 Слушаем изменения из Kafka...\n")

for message in consumer:
    payload = message.value

    if payload.get("op") == "c":
        after = payload.get("after")
        print(f"🆕 Вставка: id={after['id']}, name={after['name']}, created_at={after['created_at']}")
    elif payload.get("op") == "u":
        print("🔁 Обновление:", payload)
    elif payload.get("op") == "d":
        print("❌ Удаление:", payload)
    else:
        print("ℹ️ Другое событие:", payload)