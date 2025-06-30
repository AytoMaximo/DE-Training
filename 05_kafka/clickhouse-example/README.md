# Kafka Data Pipeline: PostgreSQL → Kafka → ClickHouse

## Описание

Этот пайплайн предназначен для передачи пользовательских событий (логины, регистрации, покупки и т.п.) из таблицы `user_logins` в базе данных PostgreSQL в ClickHouse с использованием Apache Kafka в качестве промежуточного звена.

Механизм защищён от повторной отправки данных с помощью логического поля `sent_to_kafka` в PostgreSQL и параметра `group_id` в Kafka Consumer.

---

## Компоненты пайплайна

### 1. **PostgreSQL (producer.py)**

- Источник данных: таблица `user_logins`
- Извлекаются только записи, где `sent_to_kafka = FALSE`
- Каждое событие сериализуется в JSON и отправляется в Kafka в топик `user_events`
- После отправки запись помечается как отправленная (`sent_to_kafka = TRUE`)

### 2. **Kafka**

- Топик: `user_events`
- Брокер по умолчанию: `localhost:9092`

### 3. **ClickHouse (consumer.py)**

- Подписка на топик `user_events` с `group_id="user-logins-consumer"`
- Каждое сообщение записывается в таблицу `user_logins` в ClickHouse
- Таблица создаётся автоматически, если не существует

---

## Как запустить

### 1. Запустите Kafka и Zookeeper (например, через `docker-compose`):

```bash
bash
CopyEdit
docker-compose up -d

```

### 2. Убедитесь, что PostgreSQL запущен, и в нём есть таблица `user_logins`:

```sql
sql
CopyEdit
CREATE TABLE user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
);

```

### 3. Установите зависимости:

```bash
bash
CopyEdit
pip install kafka-python psycopg2 clickhouse-connect

```

### 4. Запустите **consumer** (потребитель Kafka):

```bash
bash
CopyEdit
python consumer.py

```

> Он подключится к Kafka, создаст таблицу в ClickHouse (если её нет) и начнёт ждать сообщений.
>

### 5. Запустите **producer** (отправитель событий из PostgreSQL в Kafka):

```bash
bash
CopyEdit
python producer.py

```

> Скрипт отправит только новые записи (sent_to_kafka = FALSE), обновит их статус, и сообщения будут автоматически переданы в ClickHouse.
>

---

## Примечания

- Повторная отправка предотвращается благодаря:
    - `sent_to_kafka = TRUE` в PostgreSQL
    - `group_id` в Kafka Consumer (чтение с нужного offset'а)
- Время событий передаётся в формате Unix timestamp, затем преобразуется в `DateTime` внутри ClickHouse.
