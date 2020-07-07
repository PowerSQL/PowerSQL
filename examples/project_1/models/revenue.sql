CREATE VIEW revenue AS
SELECT CAST(product_id AS VARCHAR) product_id,
    euro
FROM product_sales;
CREATE MATERIALIZED VIEW rev_per_product AS
SELECT SUM(euro) AS rev,
    COUNT(*) quantity,
    product_id
FROM revenue
GROUP BY product_id;
--SELECT * FROM rev_per_product ORDER by quantity DESC LIMIT 100;
