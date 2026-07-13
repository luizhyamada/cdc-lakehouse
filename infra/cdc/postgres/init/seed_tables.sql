INSERT INTO customers (name, city, email) VALUES
('John Smith', 'New York', 'john.smith@email.com'),
('Emma Johnson', 'Chicago', 'emma.johnson@email.com'),
('Michael Brown', 'Dallas', 'michael.brown@email.com'),
('Sophia Davis', 'Miami', 'sophia.davis@email.com'),
('William Miller', 'Seattle', 'william.miller@email.com'),
('Olivia Wilson', 'Boston', 'olivia.wilson@email.com'),
('James Moore', 'Denver', 'james.moore@email.com'),
('Ava Taylor', 'Phoenix', 'ava.taylor@email.com'),
('Benjamin Anderson', 'Atlanta', 'ben.anderson@email.com'),
('Isabella Thomas', 'San Diego', 'isabella.thomas@email.com');

INSERT INTO products (
    product_name,
    category,
    brand,
    price,
    stock_quantity,
    active
)
VALUES
('Mechanical Keyboard K8', 'Electronics', 'Keychron', 599.90, 50, TRUE),
('Gaming Mouse G502', 'Electronics', 'Logitech', 349.90, 80, TRUE),
('27 Inch Monitor', 'Electronics', 'Dell', 1899.90, 20, TRUE),
('Wireless Earbuds', 'Electronics', 'Samsung', 799.90, 100, TRUE),
('Running Shoes', 'Sports', 'Nike', 499.90, 75, TRUE),
('Football Jersey', 'Sports', 'Adidas', 249.90, 60, TRUE),
('Coffee Maker', 'Home', 'Philips', 399.90, 30, TRUE),
('Office Chair', 'Home', 'Flexform', 1299.90, 15, TRUE),
('Python Programming Book', 'Books', 'OReilly', 149.90, 120, TRUE),
('Gaming Headset', 'Gaming', 'HyperX', 449.90, 45, TRUE);

INSERT INTO orders (customer_id, product_id, quantity, unit_price, amount, status)
SELECT c.customer_id, p.product_id, o.quantity, o.unit_price, o.amount, o.status
FROM (VALUES
    ('john.smith@email.com',   'Mechanical Keyboard K8',   1, 599.90,  599.90,  'PENDING'),
    ('emma.johnson@email.com', 'Gaming Mouse G502',        2, 349.90,  699.80,  'PAID'),
    ('michael.brown@email.com','27 Inch Monitor',          1, 1899.90, 1899.90, 'SHIPPED'),
    ('sophia.davis@email.com', 'Wireless Earbuds',         3, 799.90,  2399.70, 'PENDING'),
    ('william.miller@email.com','Running Shoes',           2, 499.90,  999.80,  'DELIVERED'),
    ('olivia.wilson@email.com','Football Jersey',          1, 249.90,  249.90,  'PAID'),
    ('james.moore@email.com', 'Coffee Maker',              4, 399.90,  1599.60, 'PENDING'),
    ('ava.taylor@email.com',  'Office Chair',              1, 1299.90, 1299.90, 'SHIPPED'),
    ('ben.anderson@email.com','Python Programming Book',   2, 149.90,  299.80,  'DELIVERED'),
    ('isabella.thomas@email.com','Gaming Headset',         1, 449.90,  449.90,  'CANCELLED')
) AS o(email, product_name, quantity, unit_price, amount, status)
JOIN customers c ON c.email = o.email
JOIN products  p ON p.product_name = o.product_name;