ASSERT NOT EXISTS (
    SELECT 1
    FROM rev_per_product
        LEFT JOIN revenue ON rev_per_product.product_id = revenue.product_id
    WHERE revenue.product_id IS NULL
) AS 'Referential integrity rev_per_product on product_id';
ASSERT NOT EXISTS (
    SELECT euro
    FROM revenue
    WHERE euro < 0
) AS 'euro should be positive';
ASSERT NOT EXISTS (
    SELECT quantity
    FROM rev_per_product
    WHERE quantity <= 0
) AS 'quantity should be positive';
