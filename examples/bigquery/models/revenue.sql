CREATE VIEW revenue AS
SELECT CAST(product_id AS STRING) product_id,
    euro
FROM (SELECT 'a' as product_id, 1.0 as euro);
CREATE TABLE rev_per_product AS
SELECT SUM(euro) AS rev,
    COUNT(*) quantity,
    product_id
FROM revenue
GROUP BY product_id;
