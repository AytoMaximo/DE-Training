INSERT INTO site_submissions
SELECT
    generateUUIDv4() AS id,
    concat('User_', toString(number)) AS full_name,
    toDate(now() + INTERVAL (rand() % 3650) DAY) AS birth_date,  -- День рождения из будущего
    round(rand() % 10000 / 100.0, 2) AS balance,  -- Баланс до 100.00
    concat('user', toString(number), '@example.com') AS email,
    arrayElement(['USA', 'Canada', 'Germany', 'France', 'UK'], (rand() % 5) + 1) AS country,
    now() - INTERVAL rand() % 100000 SECOND AS submitted_at
FROM numbers(1);

INSERT INTO site_submissions
SELECT
    generateUUIDv4() AS id,
    '' AS full_name,
    toDate(now() - INTERVAL (rand() % 3650) DAY) AS birth_date,
    round(rand() % 10000 / 100.0, 2) AS balance,  -- Баланс до 100.00
    concat('user', toString(number), '@example.com') AS email,
    arrayElement(['USA', 'Canada', 'Germany', 'France', 'UK'], (rand() % 5) + 1) AS country,
    now() - INTERVAL rand() % 86000 SECOND AS submitted_at
FROM numbers(1);