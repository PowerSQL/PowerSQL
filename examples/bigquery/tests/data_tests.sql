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
ASSERT
1 = 2
AS 'Table must contain more than 0 rows.';
