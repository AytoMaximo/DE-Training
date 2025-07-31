CREATE TABLE IF NOT EXISTS data_check (
    id UInt32,
    value Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;


INSERT INTO data_check
SELECT
    number + 1 AS id,
    round(rand() % 100, 2) AS value,
    now() AS created_at
FROM numbers(100);

INSERT INTO data_check
SELECT
    number + 101 AS id,  -- чтобы не пересекались с предыдущими 100 (id: 1–100)
    (rand() % 100) * 1.0 AS value,
    now() - INTERVAL 1 DAY AS created_at
FROM numbers(105);

INSERT INTO data_check
SELECT
    number + 1 AS id,
    round(rand() % 100, 2) AS value,
    now() AS created_at
FROM numbers(100);