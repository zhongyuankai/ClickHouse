-- A tuple compared with a Nullable tuple that has an untyped NULL element is NULL, as without Nullable,
-- so a comparison and its negation (or its inverse operator) never both filter a row out.

SELECT (1, 2) = toNullable((1, NULL)), (1, 2) != toNullable((1, NULL)), (1, 2) < toNullable((2, NULL)),
       (1, 2) > toNullable((0, NULL)), (1, 2) <= toNullable((0, NULL)), (1, 2) >= toNullable((2, NULL));
SELECT toNullable((1, NULL)) = (1, 2), toNullable((NULL, NULL)) = (1, 2), toNullable((1, NULL)) = toNullable((1, 2));
SELECT ((1, 2), 3) = (toNullable((1, NULL)), 3), (toLowCardinality('a'), 1) = toNullable(('a', NULL));
SELECT groupArray(isNull(r)) FROM (SELECT (1, 2) > if(number = 0, NULL, (0, NULL)) AS r FROM numbers(3));
SELECT groupArray(isNull(r)) FROM (SELECT materialize((1, 2)) = materialize(toNullable((1, NULL))) AS r FROM numbers(2));
SELECT toTypeName((1, 2) > toNullable((0, NULL)));

-- Not affected: null-safe comparison and a typed NULL element.
SELECT (1, NULL) <=> toNullable((1, NULL)), (1, 2) <=> toNullable((1, NULL)), (1, 2) > toNullable((0, NULL::Nullable(Int32)));

-- p, NOT p and p IS NULL together count every row; `_partition_value` filters parts by the inverted comparison.
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (dt DateTime, i Int32, j String) ENGINE = MergeTree PARTITION BY (toDate(dt), i % 2, length(j)) ORDER BY i;
INSERT INTO tbl VALUES ('2021-04-01 00:01:02', 1, '123'), ('2021-04-01 01:01:02', 1, '12'), ('2021-04-01 02:11:02', 2, '345'),
    ('2021-04-01 04:31:02', 2, '2'), ('2021-04-02 00:01:02', 1, '1234'), ('2021-04-02 00:01:02', 2, '123'),
    ('2021-04-02 00:01:02', 3, '12'), ('2021-04-02 00:01:02', 4, '1');
SELECT
    (SELECT count() FROM tbl WHERE _partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE NOT (_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL)))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))));
-- Without the count optimizations the read itself filters the parts.
SELECT
    (SELECT count() FROM tbl WHERE _partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE NOT (_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL)))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))))
SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT
    (SELECT count() FROM tbl WHERE _partition_value = toNullable((toDate('2021-04-01'), NULL, NULL))),
    (SELECT count() FROM tbl WHERE NOT (_partition_value = toNullable((toDate('2021-04-01'), NULL, NULL)))),
    (SELECT count() FROM tbl WHERE isNull(_partition_value = toNullable((toDate('2021-04-01'), NULL, NULL))))
SETTINGS optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT countMerge(s) FROM (
    SELECT countState() AS s FROM tbl WHERE _partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))
    UNION ALL SELECT countState() AS s FROM tbl WHERE NOT (_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL)))
    UNION ALL SELECT countState() AS s FROM tbl WHERE isNull(_partition_value > toNullable((toDate('2021-04-01'), NULL, NULL))));
DROP TABLE tbl;
