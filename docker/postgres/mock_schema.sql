-- Mock PostgreSQL Schema for Testing
-- This schema demonstrates a simple 3NF structure with vertex-like and edge-like tables

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS follows CASCADE;
DROP TABLE IF EXISTS purchases CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Vertex-like tables (entities)

-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE users IS 'User accounts in the system';
COMMENT ON COLUMN users.id IS 'Unique user identifier';
COMMENT ON COLUMN users.name IS 'User full name';
COMMENT ON COLUMN users.email IS 'User email address (unique)';
COMMENT ON COLUMN users.created_at IS 'Account creation timestamp';

-- Products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE products IS 'Product catalog';
COMMENT ON COLUMN products.id IS 'Unique product identifier';
COMMENT ON COLUMN products.name IS 'Product name';
COMMENT ON COLUMN products.price IS 'Product price in USD';
COMMENT ON COLUMN products.description IS 'Product description';
COMMENT ON COLUMN products.created_at IS 'Product creation timestamp';

-- Edge-like tables (relationships)

-- Purchases table (relationship between users and products)
CREATE TABLE purchases (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    quantity INTEGER DEFAULT 1,
    total_amount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
);

COMMENT ON TABLE purchases IS 'Purchase transactions linking users to products';
COMMENT ON COLUMN purchases.id IS 'Unique purchase identifier';
COMMENT ON COLUMN purchases.user_id IS 'Reference to the purchasing user';
COMMENT ON COLUMN purchases.product_id IS 'Reference to the purchased product';
COMMENT ON COLUMN purchases.purchase_date IS 'Date and time of purchase';
COMMENT ON COLUMN purchases.quantity IS 'Number of items purchased';
COMMENT ON COLUMN purchases.total_amount IS 'Total purchase amount';

-- Follows table (self-referential relationship between users)
CREATE TABLE follows (
    id SERIAL PRIMARY KEY,
    follower_id INTEGER NOT NULL,
    followed_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (follower_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (followed_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT no_self_follow CHECK (follower_id != followed_id),
    CONSTRAINT unique_follow UNIQUE (follower_id, followed_id)
);

COMMENT ON TABLE follows IS 'User follow relationships (who follows whom)';
COMMENT ON COLUMN follows.id IS 'Unique follow relationship identifier';
COMMENT ON COLUMN follows.follower_id IS 'User who is following';
COMMENT ON COLUMN follows.followed_id IS 'User being followed';
COMMENT ON COLUMN follows.created_at IS 'When the follow relationship was created';

-- Create indexes for better query performance
CREATE INDEX idx_purchases_user_id ON purchases(user_id);
CREATE INDEX idx_purchases_product_id ON purchases(product_id);
CREATE INDEX idx_purchases_purchase_date ON purchases(purchase_date);
CREATE INDEX idx_follows_follower_id ON follows(follower_id);
CREATE INDEX idx_follows_followed_id ON follows(followed_id);
CREATE INDEX idx_users_email ON users(email);

-- Insert sample data
INSERT INTO users (name, email) VALUES
    ('Alice Johnson', 'alice@example.com'),
    ('Bob Smith', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com'),
    ('Diana Prince', 'diana@example.com');

INSERT INTO products (name, price, description) VALUES
    ('Laptop', 999.99, 'High-performance laptop computer'),
    ('Mouse', 29.99, 'Wireless computer mouse'),
    ('Keyboard', 79.99, 'Mechanical keyboard'),
    ('Monitor', 299.99, '27-inch 4K monitor');

INSERT INTO purchases (user_id, product_id, quantity, total_amount) VALUES
    (1, 1, 1, 999.99),
    (1, 2, 2, 59.98),
    (2, 3, 1, 79.99),
    (2, 4, 1, 299.99),
    (3, 1, 1, 999.99),
    (3, 2, 1, 29.99);

INSERT INTO follows (follower_id, followed_id) VALUES
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 4),
    (3, 1),
    (4, 2);
