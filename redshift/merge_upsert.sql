-- Example upsert into dimensions/facts using staging as source.
-- Requires Redshift with MERGE support.
CREATE TABLE IF NOT EXISTS ca_pro.dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100),
    email      VARCHAR(256),
    country    VARCHAR(50),
    signup_date DATE
);

MERGE INTO ca_pro.dim_customer AS d
USING (
    SELECT DISTINCT customer_id, first_name, last_name, email, country, signup_date
    FROM ca_pro.stg_customer
) s
ON d.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    first_name = s.first_name,
    last_name = s.last_name,
    email = s.email,
    country = s.country,
    signup_date = s.signup_date
WHEN NOT MATCHED THEN INSERT (customer_id, first_name, last_name, email, country, signup_date)
VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.country, s.signup_date);

CREATE TABLE IF NOT EXISTS ca_pro.fact_revenue (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id INT REFERENCES ca_pro.dim_customer(customer_id),
    order_date DATE,
    amount DOUBLE PRECISION,
    payment_method VARCHAR(20),
    channel VARCHAR(20),
    ingest_date DATE
);

MERGE INTO ca_pro.fact_revenue AS f
USING ca_pro.stg_transactions s
ON f.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    customer_id = s.customer_id,
    order_date = s.order_date,
    amount = s.amount,
    payment_method = s.payment_method,
    channel = s.channel,
    ingest_date = s.ingest_date
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_date, amount, payment_method, channel, ingest_date)
VALUES (s.order_id, s.customer_id, s.order_date, s.amount, s.payment_method, s.channel, s.ingest_date);
