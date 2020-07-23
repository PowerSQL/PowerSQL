CREATE VIEW revenue AS
SELECT CAST('abc' AS VARCHAR) AS product_id,
    1.0 AS euro;
CREATE VIEW rev_per_product AS
SELECT SUM(euro) AS rev,
    COUNT(*) quantity,
    product_id
FROM revenue
GROUP BY product_id;
