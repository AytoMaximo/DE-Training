CREATE TABLE IF NOT EXISTS users (
    user_id UInt32,
    name String,
    country String,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS user_events (
    event_id UUID,
    user_id UInt32,
    event_type LowCardinality(String),
    event_time DateTime,
    metadata String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id);

CREATE TABLE IF NOT EXISTS orders (
    order_id UInt64,
    user_id UInt32,
    amount Float32,
    currency String,
    order_time DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_time, user_id);

INSERT INTO users
SELECT
    number + 1 AS user_id,
    concat('User_', toString(number)) AS name,
    arrayElement(['USA', 'Canada', 'Germany', 'France', 'UK'], intDiv(number, 200) % 5 + 1) AS country,
    now() - INTERVAL intDiv(number, 20) DAY AS created_at
FROM numbers(1000);

INSERT INTO user_events
SELECT
    generateUUIDv4() AS event_id,
    rand() % 1000 + 1 AS user_id,
    arrayElement(['login', 'logout', 'purchase', 'view'], (rand() % 4) + 1) AS event_type,
    now() - INTERVAL rand() % 10000 SECOND AS event_time,
    concat('metadata_', toString(number)) AS metadata
FROM numbers(500000);

INSERT INTO orders
SELECT
    number + 1000 AS order_id,
    rand() % 1000 + 1 AS user_id,
    round(rand() % 50000 / 100.0, 2) AS amount,
    arrayElement(['USD', 'EUR', 'GBP'], (rand() % 3) + 1) AS currency,
    now() - INTERVAL rand() % 50000 SECOND AS order_time
FROM numbers(100000);