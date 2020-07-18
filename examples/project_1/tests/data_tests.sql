--Fails, there are 2 products which are 
SELECT 1
FROM rev_per_product
    LEFT JOIN revenue ON rev_per_product.product_id = revenue.product_id
WHERE revenue.product_id IS NULL;
SELECT 1
FROM revenue
WHERE euro <= 0;
SELECT 1
FROM rev_per_product
WHERE quantity <= 0;
ASSERT NOT EXISTS (
    SELECT quantity
    FROM rev_per_product
    WHERE quantity <= 0
) AS 'quantity should be positive';
ASSERT NOT EXISTS (
    SELECT product_id
    FROM rev_per_product
    WHERE product_id IS NULL
) AS 'product_id should be not null';
ASSERT (
    SELECT COUNT (*)
    FROM rev_per_product
    WHERE quantity < 10
) >= 0.7 * (
    SELECT COUNT(*)
    FROM rev_per_product
) AS 'At least 70% should have a quantity lower than 0.7'
