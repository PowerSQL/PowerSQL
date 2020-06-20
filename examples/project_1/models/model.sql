CREATE MATERIALIZED VIEW model AS
WITH t AS (
    SELECT
        'example' AS b,
        123 AS c
)
SELECT
    1 AS a,
    2 AS b,
    c AS x
FROM
    -- this is an external
    t;
