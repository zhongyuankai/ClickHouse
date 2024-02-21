-- Tags: no-parallel, no-fasttest

SELECT 'Test not specify unique key and version';
DROP TABLE IF EXISTS unique_merge_tree;
CREATE TABLE unique_merge_tree(n1 UInt32, n2 UInt32, s String) ENGINE = UniqueMergeTree ORDER BY n1;

SELECT '=======Insert 1=======';
INSERT INTO unique_merge_tree SELECT number, 1, 'hello' FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO unique_merge_tree SELECT number, 2, 'world' FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO unique_merge_tree SELECT number, 3, 'hello, world' FROM numbers(5, 5);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;
select count(n1) from unique_merge_tree;

SELECT '=======Drop partition=======';
ALTER TABLE unique_merge_tree DROP PARTITION tuple();
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert after drop partition=======';
INSERT INTO unique_merge_tree SELECT number, 1, 'hello' FROM numbers(5, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Merge=======';
INSERT INTO unique_merge_tree SELECT number, 2, 'world' FROM numbers(10, 10);
INSERT INTO unique_merge_tree SELECT number, 3, 'hello, world' FROM numbers(15, 5);
OPTIMIZE TABLE unique_merge_tree FINAL;
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;
select count(n1) from unique_merge_tree;

SELECT '=======Truncate table=======';
TRUNCATE TABLE unique_merge_tree;
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert after truncate table=======';
INSERT INTO unique_merge_tree SELECT number, 1, 'hello' FROM numbers(20, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT 'Test not specify version';
DROP TABLE IF EXISTS unique_merge_tree;
CREATE TABLE unique_merge_tree(n1 UInt32, n2 UInt32, s String) ENGINE = UniqueMergeTree(n1) ORDER BY n1;

SELECT '=======Insert 1=======';
INSERT INTO unique_merge_tree SELECT number, 1, 'hello' FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO unique_merge_tree SELECT number, 2, 'world' FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO unique_merge_tree SELECT number, 3, 'hello, world' FROM numbers(5, 5);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Drop partition=======';
ALTER TABLE unique_merge_tree DROP PARTITION tuple();
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert after drop partition=======';
INSERT INTO unique_merge_tree SELECT number, 1, 'hello' FROM numbers(5, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Merge=======';
INSERT INTO unique_merge_tree SELECT number, 2, 'world' FROM numbers(10, 10);
INSERT INTO unique_merge_tree SELECT number, 3, 'hello, world' FROM numbers(15, 5);
OPTIMIZE TABLE unique_merge_tree FINAL;
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;
select count(n1) from unique_merge_tree;

SELECT '=======Truncate table=======';
TRUNCATE TABLE unique_merge_tree;
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert after truncate table=======';
INSERT INTO unique_merge_tree SELECT number, 1, 'hello' FROM numbers(20, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;


SELECT 'Test specify unique key and version';
DROP TABLE IF EXISTS unique_merge_tree;
CREATE TABLE unique_merge_tree(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = UniqueMergeTree(n1, v) ORDER BY n1;

SELECT '=======Insert 1=======';
INSERT INTO unique_merge_tree SELECT number, number + 1, 'a', number % 2 FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO unique_merge_tree SELECT number, number + 1, 'b', number FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO unique_merge_tree SELECT number, number + 1, 'c', number FROM numbers(5, 5);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;
select count(n1) from unique_merge_tree;

SELECT '=======Drop partition=======';
ALTER TABLE unique_merge_tree DROP PARTITION tuple();
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert after drop partition=======';
INSERT INTO unique_merge_tree SELECT number, number + 1, 'a', number % 2 FROM numbers(5, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Merge=======';
INSERT INTO unique_merge_tree SELECT number, number + 1, 'b', number FROM numbers(10, 10);
INSERT INTO unique_merge_tree SELECT number, number + 1, 'c', number FROM numbers(15, 5);
OPTIMIZE TABLE unique_merge_tree FINAL;
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;
select count(n1) from unique_merge_tree;

SELECT '=======Truncate table=======';
TRUNCATE TABLE unique_merge_tree;
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert after truncate table=======';
INSERT INTO unique_merge_tree SELECT number, number + 1, 'a', number % 2 FROM numbers(20, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;


SELECT 'Test sort key and unique key are different';
DROP TABLE IF EXISTS unique_merge_tree;
CREATE TABLE unique_merge_tree(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = UniqueMergeTree(n1, v) ORDER BY n2;

SELECT '=======Insert 1=======';
INSERT INTO unique_merge_tree SELECT number, 10 - number, 'a', number % 2 FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO unique_merge_tree SELECT number, 10 - number, 'b', number FROM numbers(10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO unique_merge_tree SELECT number, 15 - number, 'c', 5 FROM numbers(10);
INSERT INTO unique_merge_tree SELECT number, 20 - number, 'd', number % 10 FROM numbers(10, 10);
INSERT INTO unique_merge_tree SELECT number, 25 - number, 'e', number % 10 FROM numbers(15, 10);
INSERT INTO unique_merge_tree SELECT number, 30 - number, 'f', number % 10 FROM numbers(20, 10);
INSERT INTO unique_merge_tree SELECT number, 35 - number, 'g', number % 10 FROM numbers(25, 10);
SELECT *, _unique_key_id FROM unique_merge_tree ORDER BY n1;
select count(n1) from unique_merge_tree;

SELECT '=======Select=======';
SELECT s, sum(n1), count(n2) from unique_merge_tree group by s order by s;

SELECT 'Test other partition operations';
ALTER TABLE unique_merge_tree ATTACH PARTITION tuple(); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS test_tb;
CREATE TABLE test_tb(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = UniqueMergeTree(n1, v) ORDER BY n2;
INSERT INTO test_tb SELECT number, 30 - number, 'f', number % 10 FROM numbers(20, 10);
ALTER TABLE unique_merge_tree REPLACE PARTITION tuple() FROM test_tb; -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS unique_merge_tree;
