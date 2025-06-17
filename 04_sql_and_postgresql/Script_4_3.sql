-- 1. Сырая таблица (30 дней)
CREATE TABLE user_events_raw (
    user_id      UInt32,
    event_type   String,
    points_spent UInt32,
    event_time   DateTime
) ENGINE = MergeTree()
  ORDER BY (event_time, user_id)
  TTL event_time + INTERVAL 30 DAY
  SETTINGS index_granularity = 8192;

-- 2. Агрегированная таблица (180 дней)
CREATE TABLE user_events_agg (
    event_date           Date,
    event_type           String,
    unique_users_state   AggregateFunction(uniqExact, UInt32),
    points_spent_state   AggregateFunction(sum, UInt32),
    actions_count_state  AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY
SETTINGS index_granularity = 1024;

-- 3. Материализованное представление
CREATE MATERIALIZED VIEW user_events_mv
TO user_events_agg
AS
SELECT
    toDate(event_time)               AS event_date,
    event_type,
    uniqExactState(user_id)          AS unique_users_state,
    sumState(points_spent)           AS points_spent_state,
    countState()                     AS actions_count_state
FROM user_events_raw
GROUP BY event_date, event_type;

-- 4. Запрос для вставки тестовых данных
INSERT INTO user_events_raw VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- 5. Финализация
SELECT
    event_date,
    event_type,
    uniqExactMerge(unique_users_state)  AS unique_users,
    sumMerge(points_spent_state)        AS total_spent,
    countMerge(actions_count_state)     AS total_actions
FROM user_events_agg
GROUP BY
    event_date,
    event_type
ORDER BY
    event_date,
    event_type;

--|event_date|event_type|unique_users|total_spent|total_actions|
--|----------|----------|------------|-----------|-------------|
--|2025-06-07|login     |2           |0          |2            |
--|2025-06-07|signup    |1           |0          |1            |
--|2025-06-10|login     |2           |0          |2            |
--|2025-06-10|purchase  |1           |30         |1            |
--|2025-06-12|login     |1           |0          |1            |
--|2025-06-12|logout    |1           |0          |1            |
--|2025-06-12|purchase  |1           |50         |1            |
--|2025-06-14|login     |1           |0          |1            |
--|2025-06-14|purchase  |1           |70         |1            |
--|2025-06-14|signup    |1           |0          |1            |
--|2025-06-16|login     |1           |0          |1            |
--|2025-06-16|logout    |1           |0          |1            |
--|2025-06-16|purchase  |1           |20         |1            |
--|2025-06-17|login     |1           |0          |1            |
--|2025-06-17|logout    |1           |0          |1            |
--|2025-06-17|purchase  |2           |125        |2            |
--|2025-06-17|signup    |1           |0          |1            |

-- 6. Расчет Retention в течение 7 дней
WITH events AS
(
    SELECT
        user_id,
        toDate(event_time) AS event_date
    FROM user_events_raw
),
-- день первой активности каждого пользователя (cohort_date)
first_visit AS
(
    SELECT
        user_id,
        min(event_date) AS cohort_date
    FROM events
    GROUP BY user_id
),
-- были ли повторы в течение 7 дней
returned AS
(
    SELECT
        f.cohort_date,
        uniqExact(e.user_id) AS returned_in_7_days
    FROM first_visit f
    JOIN events e ON e.user_id = f.user_id
    WHERE dateDiff('day', f.cohort_date, e.event_date) BETWEEN 1 AND 7
    GROUP BY f.cohort_date
),
cohort_size AS
(
    SELECT
        cohort_date,
        uniqExact(user_id) AS total_users_day_0
    FROM first_visit
    GROUP BY cohort_date
)
SELECT
    c.cohort_date                           AS event_date,
    c.total_users_day_0,
    coalesce(r.returned_in_7_days, 0)       AS returned_in_7_days,
    round(returned_in_7_days / total_users_day_0 * 100, 2)
        AS retention_7d_percent
FROM cohort_size c
LEFT JOIN returned r USING (cohort_date)
ORDER BY event_date;

--|event_date|total_users_day_0|returned_in_7_days|retention_7d_percent|
--|----------|-----------------|------------------|--------------------|
--|2025-06-07|3                |3                 |100                 |
--|2025-06-12|1                |1                 |100                 |
--|2025-06-14|1                |1                 |100                 |
--|2025-06-17|1                |0                 |0                   |

