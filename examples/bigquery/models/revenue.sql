CREATE OR REPLACE VIEW revenue AS
SELECT CAST('my_product' AS STRING) product_id,
    1.0 AS euro;
CREATE OR REPLACE TABLE rev_per_product AS
SELECT SUM(euro) AS rev,
    COUNT(*) quantity,
    product_id
FROM revenue
GROUP BY product_id;
