CREATE SCHEMA IF NOT EXISTS ca_pro;

CREATE TABLE IF NOT EXISTS ca_pro.stg_customer (
    customer_id INT,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    email      VARCHAR(256),
    signup_date DATE,
    country     VARCHAR(50),
    lifetime_value DOUBLE PRECISION,
    ingest_date DATE
);

CREATE TABLE IF NOT EXISTS ca_pro.stg_transactions (
    order_id VARCHAR(64),
    customer_id INT,
    order_date DATE,
    amount DOUBLE PRECISION,
    payment_method VARCHAR(20),
    channel VARCHAR(20),
    ingest_date DATE
);
