-- PostgreSQL platform DB — dlt demo seed data

CREATE TABLE IF NOT EXISTS customers (
    id         SERIAL          PRIMARY KEY,
    name       VARCHAR(255)    NOT NULL,
    email      VARCHAR(255)    NOT NULL UNIQUE,
    phone      VARCHAR(50),
    address    VARCHAR(500),
    created_at TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL         PRIMARY KEY,
    name        VARCHAR(255)   NOT NULL,
    description TEXT,
    price       DECIMAL(10,2)  NOT NULL,
    stock_qty   INT            NOT NULL DEFAULT 0,
    created_at  TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL         PRIMARY KEY,
    customer_id  INT            NOT NULL REFERENCES customers(id),
    status       VARCHAR(50)    NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(10,2)  NOT NULL,
    order_date   TIMESTAMP      NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- ─── seed data ──────────────────────────────────────────────────────────────────

INSERT INTO customers (name, email, phone, address) VALUES
    ('Alice Nguyen',  'alice@example.com',  '+84901111111', '123 Le Loi, Ho Chi Minh City'),
    ('Bob Tran',      'bob@example.com',    '+84902222222', '456 Nguyen Hue, Ho Chi Minh City'),
    ('Carol Pham',    'carol@example.com',  '+84903333333', '789 Dong Khoi, Ho Chi Minh City'),
    ('David Le',      'david@example.com',  '+84904444444', '321 Hai Ba Trung, Hanoi'),
    ('Eva Hoang',     'eva@example.com',    '+84905555555', '654 Tran Hung Dao, Hanoi');

INSERT INTO products (name, description, price, stock_qty) VALUES
    ('iPhone 15 Pro',      'Apple flagship smartphone',    999.00,  50),
    ('Samsung Galaxy S24', 'Android flagship smartphone',  849.00,  75),
    ('MacBook Pro 14"',    'Apple M3 laptop',             1999.00,  30),
    ('Dell XPS 15',        'Intel Core i9 laptop',        1499.00,  40),
    ('Levi 501 Jeans',     'Classic blue denim jeans',      89.99, 200),
    ('Clean Code',         'Robert C. Martin',              45.00, 150),
    ('The Pragmatic Prog', 'Hunt & Thomas',                 42.00, 120),
    ('Nike Air Max',       'Running shoes',                129.99,  80);

-- order_date uses NOW() - INTERVAL so the full-load always returns rows
INSERT INTO orders (customer_id, status, total_amount, order_date) VALUES
    (1, 'completed',  1044.00, NOW() - INTERVAL '25 days'),
    (2, 'completed',   849.00, NOW() - INTERVAL '20 days'),
    (3, 'shipped',    2044.00, NOW() - INTERVAL '15 days'),
    (4, 'pending',     219.98, NOW() - INTERVAL '10 days'),
    (5, 'completed',   129.99, NOW() - INTERVAL '7 days'),
    (1, 'processing',   87.00, NOW() - INTERVAL '5 days'),
    (2, 'cancelled',    45.00, NOW() - INTERVAL '3 days'),
    (3, 'completed',   999.00, NOW() - INTERVAL '1 day');
