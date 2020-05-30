WITH
    t
    AS
    (
        SELECT
            'example' as b,
            123 as c
    )
SELECT
    b,
    c AS x
FROM
    t
