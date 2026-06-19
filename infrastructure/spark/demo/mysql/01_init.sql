-- MySQL ecommerce DB — demo seed data

CREATE TABLE IF NOT EXISTS categories (
    id          INT           NOT NULL AUTO_INCREMENT,
    name        VARCHAR(100)  NOT NULL,
    description VARCHAR(500),
    parent_id   INT,
    created_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (parent_id) REFERENCES categories(id)
);

CREATE TABLE IF NOT EXISTS products (
    id          INT             NOT NULL AUTO_INCREMENT,
    category_id INT,
    name        VARCHAR(255)    NOT NULL,
    description TEXT,
    price       DECIMAL(10, 2)  NOT NULL,
    stock_qty   INT             NOT NULL DEFAULT 0,
    created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE IF NOT EXISTS customers (
    id          INT           NOT NULL AUTO_INCREMENT,
    name        VARCHAR(255)  NOT NULL,
    email       VARCHAR(255)  NOT NULL UNIQUE,
    phone       VARCHAR(50),
    address     VARCHAR(500),
    created_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS orders (
    id            INT             NOT NULL AUTO_INCREMENT,
    customer_id   INT             NOT NULL,
    status        VARCHAR(50)     NOT NULL DEFAULT 'pending',
    total_amount  DECIMAL(10, 2)  NOT NULL,
    order_date    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS order_items (
    id          INT             NOT NULL AUTO_INCREMENT,
    order_id    INT             NOT NULL,
    product_id  INT             NOT NULL,
    quantity    INT             NOT NULL DEFAULT 1,
    unit_price  DECIMAL(10, 2)  NOT NULL,
    subtotal    DECIMAL(10, 2)  NOT NULL,
    created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (order_id)   REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- ─── seed data ─────────────────────────────────────────────────────────────────

INSERT INTO categories (name, description, parent_id) VALUES
    ('Electronics',   'Electronic devices and accessories',  NULL),
    ('Clothing',      'Apparel and fashion',                 NULL),
    ('Books',         'Books and e-books',                   NULL),
    ('Smartphones',   'Mobile phones and accessories',       1),
    ('Laptops',       'Portable computers',                  1);

INSERT INTO products (category_id, name, description, price, stock_qty) VALUES
    (4, 'iPhone 15 Pro',      'Apple flagship smartphone',    999.00,  50),
    (4, 'Samsung Galaxy S24', 'Android flagship smartphone',  849.00,  75),
    (5, 'MacBook Pro 14"',    'Apple M3 laptop',             1999.00,  30),
    (5, 'Dell XPS 15',        'Intel Core i9 laptop',        1499.00,  40),
    (2, 'Levi 501 Jeans',     'Classic blue denim jeans',      89.99, 200),
    (3, 'Clean Code',         'Robert C. Martin',              45.00, 150),
    (3, 'The Pragmatic Prog', 'Hunt & Thomas',                 42.00, 120),
    (2, 'Nike Air Max',       'Running shoes',                129.99,  80);

INSERT INTO customers (name, email, phone, address) VALUES
    ('Alice Nguyen',  'alice@example.com',  '+84901111111', '123 Le Loi, Ho Chi Minh City'),
    ('Bob Tran',      'bob@example.com',    '+84902222222', '456 Nguyen Hue, Ho Chi Minh City'),
    ('Carol Pham',    'carol@example.com',  '+84903333333', '789 Dong Khoi, Ho Chi Minh City'),
    ('David Le',      'david@example.com',  '+84904444444', '321 Hai Ba Trung, Hanoi'),
    ('Eva Hoang',     'eva@example.com',    '+84905555555', '654 Tran Hung Dao, Hanoi');

-- order_date uses DATE_SUB so the 30-day rolling window query always returns rows
INSERT INTO orders (customer_id, status, total_amount, order_date) VALUES
    (1, 'completed', 1044.00, DATE_SUB(NOW(), INTERVAL 25 DAY)),
    (2, 'completed',  849.00, DATE_SUB(NOW(), INTERVAL 20 DAY)),
    (3, 'shipped',   2044.00, DATE_SUB(NOW(), INTERVAL 15 DAY)),
    (4, 'pending',    219.98, DATE_SUB(NOW(), INTERVAL 10 DAY)),
    (5, 'completed',  129.99, DATE_SUB(NOW(), INTERVAL 7 DAY)),
    (1, 'processing',  87.00, DATE_SUB(NOW(), INTERVAL 5 DAY)),
    (2, 'cancelled',   45.00, DATE_SUB(NOW(), INTERVAL 3 DAY)),
    (3, 'completed',  999.00, DATE_SUB(NOW(), INTERVAL 1 DAY));

INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
    (1, 1, 1,  999.00,  999.00),
    (1, 6, 1,   45.00,   45.00),
    (2, 2, 1,  849.00,  849.00),
    (3, 3, 1, 1999.00, 1999.00),
    (3, 7, 1,   45.00,   45.00),
    (4, 5, 2,   89.99,  179.98),
    (4, 6, 1,   40.00,   40.00),
    (5, 8, 1,  129.99,  129.99),
    (6, 7, 2,   42.00,   84.00),
    (7, 6, 1,   45.00,   45.00),
    (8, 1, 1,  999.00,  999.00);
