-- PostgreSQL manage DB — demo seed data

CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL     PRIMARY KEY,
    username    VARCHAR(100)  NOT NULL UNIQUE,
    email       VARCHAR(255)  NOT NULL UNIQUE,
    role        VARCHAR(50)   NOT NULL DEFAULT 'member',
    created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projects (
    id          BIGSERIAL     PRIMARY KEY,
    name        VARCHAR(255)  NOT NULL,
    description TEXT,
    owner_id    BIGINT        REFERENCES users(id),
    status      VARCHAR(50)   NOT NULL DEFAULT 'active',
    start_date  DATE,
    end_date    DATE,
    created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tasks (
    id          BIGSERIAL     PRIMARY KEY,
    project_id  BIGINT        REFERENCES projects(id),
    title       VARCHAR(500)  NOT NULL,
    description TEXT,
    assignee_id BIGINT        REFERENCES users(id),
    status      VARCHAR(50)   NOT NULL DEFAULT 'open',
    priority    VARCHAR(20)   NOT NULL DEFAULT 'medium',
    due_date    DATE,
    created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS issues (
    id          BIGSERIAL     PRIMARY KEY,
    project_id  BIGINT        REFERENCES projects(id),
    title       VARCHAR(500)  NOT NULL,
    description TEXT,
    reporter_id BIGINT        REFERENCES users(id),
    assignee_id BIGINT        REFERENCES users(id),
    status      VARCHAR(50)   NOT NULL DEFAULT 'open',
    severity    VARCHAR(20)   NOT NULL DEFAULT 'medium',
    created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS comments (
    id          BIGSERIAL     PRIMARY KEY,
    project_id  BIGINT        REFERENCES projects(id),
    author_id   BIGINT        REFERENCES users(id),
    entity_type VARCHAR(50)   NOT NULL,   -- 'task' | 'issue'
    entity_id   BIGINT        NOT NULL,
    body        TEXT          NOT NULL,
    created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- ─── seed data ─────────────────────────────────────────────────────────────────

INSERT INTO users (username, email, role) VALUES
    ('phat.nguyen',  'phat@company.com',   'admin'),
    ('alice.tran',   'alice@company.com',  'lead'),
    ('bob.le',       'bob@company.com',    'member'),
    ('carol.pham',   'carol@company.com',  'member'),
    ('david.hoang',  'david@company.com',  'viewer');

INSERT INTO projects (name, description, owner_id, status, start_date, end_date) VALUES
    ('Data Platform v2',  'Next-gen lakehouse ingestion layer',  1, 'active',    '2025-01-01', '2025-06-30'),
    ('Mobile App 3.0',    'Cross-platform mobile rewrite',       2, 'active',    '2025-02-01', '2025-08-31'),
    ('API Gateway',       'Centralised API management layer',    1, 'planning',  '2025-04-01', '2025-10-31');

INSERT INTO tasks (project_id, title, description, assignee_id, status, priority, due_date) VALUES
    (1, 'Set up Spark SDP pipeline',  'Configure PySpark 4.1 declarative pipelines', 1, 'done',        'high',   '2025-02-28'),
    (1, 'Add Bronze schema files',    'Define StructType for all source tables',      2, 'done',        'high',   '2025-03-15'),
    (1, 'Implement Silver layer',     'Cleanse and deduplicate bronze tables',        3, 'in_progress', 'medium', '2025-04-30'),
    (1, 'Set up Airflow DAGs',        'Schedule bronze and silver pipeline runs',     1, 'open',        'high',   '2025-05-15'),
    (2, 'Design app architecture',    'Define screens and data models',               2, 'done',        'high',   '2025-03-01'),
    (2, 'Implement auth flow',        'OAuth2 login with refresh tokens',             3, 'in_progress', 'high',   '2025-04-15'),
    (3, 'Rate limiting design',       'Design token bucket rate limiter',             4, 'open',        'medium', '2025-05-01'),
    (3, 'Auth middleware',            'JWT validation middleware',                    4, 'open',        'high',   '2025-05-15');

INSERT INTO issues (project_id, title, description, reporter_id, assignee_id, status, severity) VALUES
    (1, 'JDBC read timeout on large tables', 'Orders table >1M rows causes timeout',  3, 2, 'open',     'high'),
    (1, 'Schema drift on customers table',   'email column changed to nullable',       2, 1, 'resolved', 'medium'),
    (2, 'Login fails on Android 12',         'Token refresh returns 401',             4, 3, 'open',     'critical'),
    (2, 'Dark mode crashes on iOS',          'App crashes when switching theme',      5, 3, 'open',     'high'),
    (3, 'Rate limit not applied to /health', '/health endpoint bypasses rate limit',  1, 4, 'open',     'low');

INSERT INTO comments (project_id, author_id, entity_type, entity_id, body) VALUES
    (1, 2, 'issue', 1, 'Reproduced. Root cause is fetchsize=10000 with no query partitioning.'),
    (1, 1, 'issue', 1, 'Fix: add numPartitions + partitionColumn options to JDBC read.'),
    (1, 3, 'task',  3, 'Started dedup logic. Using row_number() over partition by PK.'),
    (2, 3, 'issue', 3, 'Found the bug — refresh token is not being stored in secure storage.'),
    (2, 4, 'issue', 4, 'Cannot reproduce on iOS 17. Likely iOS 16 specific.'),
    (3, 1, 'task',  7, 'Going with sliding window counter stored in Redis.'),
    (1, 2, 'task',  2, 'All schemas reviewed and merged. ingestion_date added as partition col.'),
    (3, 4, 'issue', 5, '/health should always be excluded from rate limits by convention.');
