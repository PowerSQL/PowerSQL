WITH
    t
    AS
    (
        SELECT
            'example' as b,
            123 as c
    )
SELECT
    1 AS a,
    2 AS b,
    c AS x
FROM
    t
