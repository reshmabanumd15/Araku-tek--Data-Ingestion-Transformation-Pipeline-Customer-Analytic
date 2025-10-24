CREATE VIEW ca_pro.vw_customer_value AS
SELECT d.customer_id, d.first_name, d.last_name, d.country, d.signup_date,
       SUM(f.amount) AS total_spend,
       COUNT(*) AS orders
FROM ca_pro.dim_customer d
LEFT JOIN ca_pro.fact_revenue f ON d.customer_id = f.customer_id
GROUP BY 1,2,3,4,5;
