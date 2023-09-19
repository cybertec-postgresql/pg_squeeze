CREATE EXTENSION pg_squeeze;

CREATE TABLE a(i int PRIMARY KEY, j int);

INSERT INTO a(i, j)
SELECT x, x
FROM generate_series(1, 10) AS g(x);

-- The trivial case.
SELECT squeeze.squeeze_table('public', 'a', NULL);

SELECT * FROM a;

-- Clustering by index.
CREATE INDEX a_i_idx_desc ON a(i DESC);
SELECT squeeze.squeeze_table('public', 'a', 'a_i_idx_desc');
SELECT * FROM a;

-- Involve TOAST.
CREATE TABLE b(i int PRIMARY KEY, t text);
INSERT INTO b(i, t)
SELECT x, repeat(x::text, 1024)
FROM generate_series(1, 10) AS g(x) GROUP BY x;
SELECT reltoastrelid > 0 FROM pg_class WHERE relname='b';
-- Copy the data into another table so we can check later.
CREATE TABLE b_copy (LIKE b INCLUDING ALL);
INSERT INTO b_copy(i, t) SELECT i, t FROM b;
-- Squeeze.
SELECT squeeze.squeeze_table('public', 'b', NULL);
-- Compare.
SELECT b.t = b_copy.t
FROM   b, b_copy
WHERE  b.i = b_copy.i;

