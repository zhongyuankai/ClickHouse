-- Tags: no-parallel, no-fasttest

SELECT 'Test not specify unique key and version';
DROP TABLE IF EXISTS replicated_unique_merge_tree1;
DROP TABLE IF EXISTS replicated_unique_merge_tree2;
CREATE TABLE replicated_unique_merge_tree1(n1 UInt32, n2 UInt32, s String) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree1', '1') ORDER BY n1;
CREATE TABLE replicated_unique_merge_tree2(n1 UInt32, n2 UInt32, s String) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree1', '2') ORDER BY n1;

SELECT '=======Insert 1=======';
INSERT INTO replicated_unique_merge_tree1 SELECT number, 1, 'hello' FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO replicated_unique_merge_tree1 SELECT number, 2, 'world' FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO replicated_unique_merge_tree1 SELECT number, 3, 'hello, world' FROM numbers(5, 5);
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree2;

SELECT '=======Drop partition=======';
ALTER TABLE replicated_unique_merge_tree1 DROP PARTITION tuple();
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;

SELECT '=======Insert after drop partition=======';
INSERT INTO replicated_unique_merge_tree1 SELECT number, 1, 'hello' FROM numbers(5, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;

SELECT '=======Merge=======';
INSERT INTO replicated_unique_merge_tree1 SELECT number, 2, 'world' FROM numbers(10, 10);
INSERT INTO replicated_unique_merge_tree1 SELECT number, 3, 'hello, world' FROM numbers(15, 5);
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree2;

SELECT '=======Truncate table=======';
TRUNCATE TABLE replicated_unique_merge_tree1;
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;

SELECT '=======Insert after truncate table=======';
INSERT INTO replicated_unique_merge_tree1 SELECT number, 1, 'hello' FROM numbers(20, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree2;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree2 ORDER BY n1;

SELECT 'Test not specify version';
DROP TABLE IF EXISTS replicated_unique_merge_tree3;
DROP TABLE IF EXISTS replicated_unique_merge_tree4;
CREATE TABLE replicated_unique_merge_tree3(n1 UInt32, n2 UInt32, s String) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree3', '1', n1) ORDER BY n1;
CREATE TABLE replicated_unique_merge_tree4(n1 UInt32, n2 UInt32, s String) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree3', '2', n1) ORDER BY n1;

SELECT '=======Insert 1=======';
INSERT INTO replicated_unique_merge_tree3 SELECT number, 1, 'hello' FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO replicated_unique_merge_tree3 SELECT number, 2, 'world' FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO replicated_unique_merge_tree3 SELECT number, 3, 'hello, world' FROM numbers(5, 5);
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree4;

SELECT '=======Drop partition=======';
ALTER TABLE replicated_unique_merge_tree3 DROP PARTITION tuple();
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;

SELECT '=======Insert after drop partition=======';
INSERT INTO replicated_unique_merge_tree3 SELECT number, 1, 'hello' FROM numbers(5, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;

SELECT '=======Merge=======';
INSERT INTO replicated_unique_merge_tree3 SELECT number, 2, 'world' FROM numbers(10, 10);
INSERT INTO replicated_unique_merge_tree3 SELECT number, 3, 'hello, world' FROM numbers(15, 5);
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree4;

SELECT '=======Truncate table=======';
TRUNCATE TABLE replicated_unique_merge_tree3;
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;

SELECT '=======Insert after truncate table=======';
INSERT INTO replicated_unique_merge_tree3 SELECT number, 1, 'hello' FROM numbers(20, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree4;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree4 ORDER BY n1;

SELECT 'Test specify unique key and version';
DROP TABLE IF EXISTS replicated_unique_merge_tree5;
DROP TABLE IF EXISTS replicated_unique_merge_tree6;
CREATE TABLE replicated_unique_merge_tree5(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree5', '1', n1, v) ORDER BY n1;
CREATE TABLE replicated_unique_merge_tree6(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree5', '2', n1, v) ORDER BY n1;

SELECT '=======Insert 1=======';
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'a', number % 2 FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'b', number FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'c', number FROM numbers(5, 5);
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree6;

SELECT '=======Drop partition=======';
ALTER TABLE replicated_unique_merge_tree5 DROP PARTITION tuple();
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;

SELECT '=======Insert after drop partition=======';
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'a', number % 2 FROM numbers(5, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;

SELECT '=======Merge=======';
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'b', number FROM numbers(10, 10);
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'c', number FROM numbers(15, 5);
OPTIMIZE TABLE replicated_unique_merge_tree5 FINAL;
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree6;

SELECT '=======Truncate table=======';
TRUNCATE TABLE replicated_unique_merge_tree5;
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;

SELECT '=======Insert after truncate table=======';
INSERT INTO replicated_unique_merge_tree5 SELECT number, number + 1, 'a', number % 2 FROM numbers(20, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree6;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree6 ORDER BY n1;


SELECT 'Test sort key and unique key are different';
DROP TABLE IF EXISTS replicated_unique_merge_tree7;
DROP TABLE IF EXISTS replicated_unique_merge_tree8;
CREATE TABLE replicated_unique_merge_tree7(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree7', '1', n1, v) ORDER BY n2;
CREATE TABLE replicated_unique_merge_tree8(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree7', '2', n1, v) ORDER BY n2;

SELECT '=======Insert 1=======';
INSERT INTO replicated_unique_merge_tree7 SELECT number, 10 - number, 'a', number % 2 FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree8;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree8 ORDER BY n1;

SELECT '=======Insert 2=======';
INSERT INTO replicated_unique_merge_tree7 SELECT number, 10 - number, 'b', number FROM numbers(10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree8;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree8 ORDER BY n1;

SELECT '=======Insert 3=======';
INSERT INTO replicated_unique_merge_tree7 SELECT number, 15 - number, 'c', 5 FROM numbers(10);
INSERT INTO replicated_unique_merge_tree7 SELECT number, 20 - number, 'd', number % 10 FROM numbers(10, 10);
INSERT INTO replicated_unique_merge_tree7 SELECT number, 25 - number, 'e', number % 10 FROM numbers(15, 10);
INSERT INTO replicated_unique_merge_tree7 SELECT number, 30 - number, 'f', number % 10 FROM numbers(20, 10);
INSERT INTO replicated_unique_merge_tree7 SELECT number, 35 - number, 'g', number % 10 FROM numbers(25, 10);
SYSTEM SYNC REPLICA replicated_unique_merge_tree8;
SELECT *, _unique_key_id FROM replicated_unique_merge_tree8 ORDER BY n1;
select count(n1) from replicated_unique_merge_tree8;

SELECT '=======Select 1=======';
SELECT s, sum(n1), count(n2) from replicated_unique_merge_tree8 group by s order by s;

SELECT 'Test other partition operations';
ALTER TABLE replicated_unique_merge_tree7 ATTACH PARTITION tuple(); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS replicated_unique_merge_tree9;
CREATE TABLE replicated_unique_merge_tree9(n1 UInt32, n2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_02953/replicated_unique_merge_tree9', '1', n1, v) ORDER BY n2;
INSERT INTO replicated_unique_merge_tree9 SELECT number, 30 - number, 'f', number % 10 FROM numbers(20, 10);
ALTER TABLE replicated_unique_merge_tree7 REPLACE PARTITION tuple() FROM replicated_unique_merge_tree9; -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS replicated_unique_merge_tree1;
DROP TABLE IF EXISTS replicated_unique_merge_tree2;
DROP TABLE IF EXISTS replicated_unique_merge_tree3;
DROP TABLE IF EXISTS replicated_unique_merge_tree4;
DROP TABLE IF EXISTS replicated_unique_merge_tree5;
DROP TABLE IF EXISTS replicated_unique_merge_tree6;
DROP TABLE IF EXISTS replicated_unique_merge_tree7;
DROP TABLE IF EXISTS replicated_unique_merge_tree8;
DROP TABLE IF EXISTS replicated_unique_merge_tree9;
