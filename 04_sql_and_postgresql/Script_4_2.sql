CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- Далее создадим триггерную функцию
CREATE OR REPLACE FUNCTION log_user_update()
RETURNS TRIGGER AS $$
DECLARE
    changer TEXT := current_user;
BEGIN
    -- имя
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, changer, 'name', OLD.name, NEW.name);
    END IF;

    -- email
    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, changer, 'email', OLD.email, NEW.email);
    END IF;

    -- роль
    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, changer, 'role', OLD.role, NEW.role);
    END IF;

	-- обновим метку времени в users
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- А также сделаем сам триггер.
CREATE TRIGGER trg_users_audit
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_update();

-- Далее вставим данные в таблицу users.
INSERT INTO users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'reader'),
('Anna Petrova', 'anna@example.com', 'writer');

-- И, как только мы обновим данные - сработает триггер. Сделаем это командой - 
UPDATE users
SET email = 'ivan.new@example.com'
WHERE name = 'Ivan Ivanov';

UPDATE users
SET role = 'admin'
WHERE name = 'Anna Petrova';

-- Подключаем планировщик
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Функция экспорта «сегодняшних» данных в CSV
CREATE OR REPLACE FUNCTION export_users_audit_today()
RETURNS void AS $$
DECLARE
    out_path TEXT;
BEGIN
    -- путь + сегодняшняя дата в формате ГГГГ-ММ-ДД
    out_path := format('/tmp/users_audit_export_%s.csv',
                       to_char(current_date, 'YYYY-MM-DD'));

    -- выгружаем все записи за сегодня
    EXECUTE format($sql$
        COPY (
            SELECT *
              FROM users_audit
             WHERE changed_at::date = current_date
        )
        TO %L
        WITH (FORMAT csv, HEADER)
    $sql$, out_path);
END;
$$ LANGUAGE plpgsql;

-- Планировщик pg_cron на каждый день в 03:00
SELECT cron.schedule(
  'daily_export_users_audit',      -- имя задания
  '0 3 * * *',                     -- каждый день в 03:00
  $$SELECT export_users_audit_today();$$
);

-- Более оперативный вариант для проверки работоспособности
-- SELECT cron.schedule(
--   'test_immediate',
--   '* * * * *',
--   $$SELECT export_users_audit_today();$$
-- );

-- SELECT cron.unschedule('test_immediate');
-- SELECT cron.unschedule('daily_export_users_audit');

-- Список всех задач
SELECT
  jobid,
  schedule,
  command,
  active
FROM cron.job;




