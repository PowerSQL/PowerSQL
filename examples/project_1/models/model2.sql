CREATE MATERIALIZED VIEW model2 AS
SELECT
    a,
    b
FROM
    model;


CREATE MATERIALIZED VIEW model3 AS
SELECT
    a,
    b
FROM
    model2;
