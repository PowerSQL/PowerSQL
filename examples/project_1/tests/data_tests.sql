ASSERT NOT EXISTS (
    SELECT 1
    FROM rev_per_product
        LEFT JOIN revenue ON rev_per_product.product_id = revenue.product_id
    WHERE revenue.product_id IS NOT NULL
) AS 'Referential integrity rev_per_product on product_id';
ASSERT NOT EXISTS (
    SELECT euro
    FROM revenue
    WHERE euro < 0
) AS 'Euro should be at least zero';
ASSERT NOT EXISTS (
    SELECT quantity
    FROM rev_per_product
    WHERE quantity <= 0
) AS 'Quantity should be positive';
ASSERT NOT EXISTS (
    SELECT product_id
    FROM rev_per_product
    WHERE product_id IS NULL
) AS 'Product_id should be not null';
ASSERT (
    SELECT COUNT (*)
    FROM rev_per_product
    WHERE quantity < 10
) >= 0.7 * (
    SELECT COUNT(*)
    FROM rev_per_product
) AS 'At least 70% should have a quantity lower than 10'
