CREATE TABLE IF NOT EXISTS site_submissions (
    id UUID,
    full_name String,
    birth_date Date,
    balance Float32,
    email String,
    country String,
    submitted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY submitted_at;