from kafka import KafkaConsumer
import json
import requests
from datetime import datetime

CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "user"
CLICKHOUSE_PASSWORD = "strongpassword"

# Функция create_clickhouse_table отвечает за создание таблицы changes в базе ClickHouse.
# Она отправляет SQL-запрос через HTTP-запрос с авторизацией и создаёт таблицу, если та ещё не существует.
# Эта таблица будет хранить изменения: ID, имя, дату создания, тип операции (вставка, обновление, удаление)
# и временную метку вставки.

def create_clickhouse_table():
    query = """
    CREATE TABLE IF NOT EXISTS changes (
        id UInt32,
        name String,
        created_at DateTime,
        op String,
        ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (id, ts);
    """
    r = requests.post(
        CLICKHOUSE_URL,
        data=query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )

# Функция insert_to_clickhouse принимает список событий (batch) и формирует из них SQL-запрос
# для массовой вставки в ClickHouse. Она собирает значения из каждой записи, приводит дату к нужному формату
# и отправляет данные в виде запроса. Эта функция нужна, чтобы быстро и эффективно передавать пачку изменений в базу.

def insert_to_clickhouse(batch):
    values = []
    for row in batch:
        id = row.get("id", 0)
        name = row.get("name", "")
        created_at = row.get("created_at", datetime.now().isoformat())
        op = row.get("op", "u")
        values.append(f"({id}, '{name}', toDateTime('{created_at}'), '{op}')")

    if values:
        query = (
            "INSERT INTO changes (id, name, created_at, op) VALUES " +
            ", ".join(values)
        )
        r = requests.post(
            CLICKHOUSE_URL,
            data=query,
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
        )

# Функция select_changes выполняет простой SELECT-запрос к ClickHouse,
# чтобы вывести 10 последних записей из таблицы changes. Она также использует HTTP-запрос с логином и паролем.
# Эта функция нужна для проверки — чтобы убедиться, что данные действительно сохраняются в ClickHouse.

def select_changes():
    query = "SELECT * FROM changes ORDER BY ts DESC LIMIT 10"
    r = requests.post(
        CLICKHOUSE_URL,
        data=query,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )
    if r.status_code == 200:
        print("📥 Последние события из ClickHouse:")
        print(r.text)
    else:
        print(f"❌ Ошибка при выполнении SELECT: {r.text}")


# Функция consume_kafka_once подключается к Kafka и слушает только новые сообщения (начиная с последнего доступного).
# Она обрабатывает события Debezium, проверяя их тип (op), и формирует буфер из интересующих изменений
# (в нашем случае это только обновления). После завершения чтения, буфер отправляется в ClickHouse.
# Эта функция исполняется один раз, не бесконечно — она выходит из цикла, если новых сообщений нет в течение 20 секунд.

def consume_kafka_once():
    consumer = KafkaConsumer(
        'pgserver1.public.my_table', # Название топика, откуда читаем данные (в вашем случае — от Debezium)
        bootstrap_servers='localhost:29092',  # Адрес брокера Kafka
        auto_offset_reset='latest', # Если нет сохранённой позиции, начать с "последнего" сообщения (можно: 'earliest', 'latest', 'none')
        enable_auto_commit=False, # Не сохранять автоматически позицию чтения (offset), управляем этим вручную
        group_id='pg-consumer-once', # Идентификатор группы — нужен для отслеживания offset-ов и балансировки между несколькими consumer-ами
        value_deserializer=lambda m: json.loads(m.decode('utf-8')), # Преобразуем байты сообщения в JSON
        consumer_timeout_ms=20000 # Завершаем чтение, если в течение 20 секунд не приходит ни одного сообщения
    )

    print("🟢 Чтение сообщений из Kafka...\n")

    buffer = []

    for message in consumer:
        payload = message.value
        op = payload.get("op")

        if op == "u":
            after = payload.get("after")
            after["op"] = "u"
            print(f"🔁 Обновление: {after}")
            buffer.append(after)
        else:
            print(f"ℹ️ Пропущено (op = {op}):", payload)

    consumer.close()

    if buffer:
        insert_to_clickhouse(buffer)
    else:
        print("⚠️ Нет новых событий для записи в ClickHouse.")


if __name__ == '__main__':
    create_clickhouse_table()
    consume_kafka_once()
    select_changes()