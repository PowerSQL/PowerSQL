-- Fails, there are 2 products which are 
SELECT 1
FROM revenue
WHERE euro <= 0;
SELECT 1
FROM rev_per_product
WHERE quantity <= 0;
