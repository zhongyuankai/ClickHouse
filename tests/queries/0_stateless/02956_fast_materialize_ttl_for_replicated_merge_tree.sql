SET alter_sync = 2;

SELECT 'Test MergeTree with ttl_only_drop_part set to false.';
DROP TABLE IF EXISTS test_fast_ttl1;
CREATE TABLE test_fast_ttl1 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02956/test_fast_ttl1', '1')
PARTITION BY name
ORDER BY id
TTL create_time + toIntervalDay(300)
SETTINGS ttl_only_drop_parts = false;

DROP TABLE IF EXISTS test_fast_ttl2;
CREATE TABLE test_fast_ttl2 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02956/test_fast_ttl1', '2')
PARTITION BY name
ORDER BY id
TTL create_time + toIntervalDay(300)
SETTINGS ttl_only_drop_parts = false;

INSERT INTO test_fast_ttl1 SELECT number, 'Anna', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl1 SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl1 SELECT number, 'jan', now() from numbers(2000);

SELECT 'Before modifying TTL';
SYSTEM SYNC REPLICA test_fast_ttl2;
SELECT COUNT(*) FROM test_fast_ttl2;
ALTER TABLE test_fast_ttl1 MODIFY TTL create_time + INTERVAL 10 DAY;

SELECT 'After modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl2;
SELECT id, name FROM test_fast_ttl2 ORDER BY name, id limit 3;

SELECT 'Test merge';
INSERT INTO test_fast_ttl1 SELECT number, 'Anna', if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl1 FINAL;
SYSTEM SYNC REPLICA test_fast_ttl2;
SELECT COUNT(*) FROM test_fast_ttl2;


SELECT 'Test MergeTree with ttl_only_drop_part set to true.';
DROP TABLE IF EXISTS test_fast_ttl3;
CREATE TABLE test_fast_ttl3 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02956/test_fast_ttl3', '1')
PARTITION BY name
ORDER BY id
TTL create_time + toIntervalDay(300)
SETTINGS ttl_only_drop_parts = true;

DROP TABLE IF EXISTS test_fast_ttl4;
CREATE TABLE test_fast_ttl4 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02956/test_fast_ttl3', '2')
PARTITION BY name
ORDER BY id
TTL create_time + toIntervalDay(300)
SETTINGS ttl_only_drop_parts = true;

INSERT INTO test_fast_ttl3 SELECT number, 'Anna', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl3 SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl3 SELECT number, 'jan', now() from numbers(2000);

SELECT 'Before modifying TTL';
SYSTEM SYNC REPLICA test_fast_ttl4;
SELECT COUNT(*) FROM test_fast_ttl4;
ALTER TABLE test_fast_ttl3 MODIFY TTL create_time + INTERVAL 10 DAY;

SELECT 'After modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl4;
SELECT id, name FROM test_fast_ttl4 ORDER BY name, id limit 3;

SELECT 'Test merge';
INSERT INTO test_fast_ttl3 SELECT number, 'Anna',  if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl3 FINAL;
SYSTEM SYNC REPLICA test_fast_ttl4;
SELECT COUNT(*) FROM test_fast_ttl4;


SELECT 'Test MergeTree without ttl.';
DROP TABLE IF EXISTS test_fast_ttl5;
CREATE TABLE test_fast_ttl5 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02956/test_fast_ttl5', '1')
PARTITION BY name
ORDER BY id
SETTINGS ttl_only_drop_parts = false;

DROP TABLE IF EXISTS test_fast_ttl6;
CREATE TABLE test_fast_ttl6 (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_02956/test_fast_ttl5', '2')
PARTITION BY name
ORDER BY id
SETTINGS ttl_only_drop_parts = false;

INSERT INTO test_fast_ttl5 SELECT number, 'Anna', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl5 SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl5 SELECT number, 'jan', now() from numbers(2000);

SELECT 'Before modifying TTL';
SYSTEM SYNC REPLICA test_fast_ttl6;
SELECT COUNT(*) FROM test_fast_ttl6;

SELECT 'Add modifying TTL';
ALTER TABLE test_fast_ttl5 MODIFY TTL create_time + INTERVAL 300 DAY;
SELECT COUNT(*) FROM test_fast_ttl6;
SELECT id, name FROM test_fast_ttl6 ORDER BY name, id limit 3;

SELECT 'Shorten TTL';
ALTER TABLE test_fast_ttl5 MODIFY TTL create_time + INTERVAL 10 DAY;
SELECT COUNT(*) FROM test_fast_ttl6;
SELECT id, name FROM test_fast_ttl6 ORDER BY name, id limit 3;

SELECT 'Extend TTL';
INSERT INTO test_fast_ttl5 SELECT number, 'jan', date_sub(day, 70, now()) from numbers(2000);
INSERT INTO test_fast_ttl5 SELECT number, 'jan', if(number >= 1000, date_sub(day, 60, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl5 SELECT number, 'Anna', date_sub(day, 6, now()) from numbers(2000);
ALTER TABLE test_fast_ttl5 MODIFY TTL create_time + INTERVAL 40 DAY;

SYSTEM SYNC REPLICA test_fast_ttl6;
SELECT COUNT(*) FROM test_fast_ttl6;
SELECT id, name FROM test_fast_ttl6 ORDER BY name, id limit 3;

SELECT 'Shorten TTL and ttl_only_drop_part set to true';
ALTER TABLE test_fast_ttl5 MODIFY SETTING ttl_only_drop_parts = true;
INSERT INTO test_fast_ttl5 SELECT number, 'jan', date_sub(day, 6, now()) from numbers(2000);
INSERT INTO test_fast_ttl5 SELECT number, 'Anna', if(number >= 1000, date_sub(day, 7, now()), now()) from numbers(2000);
ALTER TABLE test_fast_ttl5 MODIFY TTL create_time + INTERVAL 2 DAY;

SELECT 'Test merge';
INSERT INTO test_fast_ttl5 SELECT number, 'Anna', if(number >= 1000, date_sub(day, 5, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl5 FINAL;
SYSTEM SYNC REPLICA test_fast_ttl6;
SELECT COUNT(*) FROM test_fast_ttl6;
SELECT id, name FROM test_fast_ttl6 ORDER BY name, id limit 3;


DROP TABLE IF EXISTS test_fast_ttl1;
DROP TABLE IF EXISTS test_fast_ttl2;
DROP TABLE IF EXISTS test_fast_ttl3;
DROP TABLE IF EXISTS test_fast_ttl4;
DROP TABLE IF EXISTS test_fast_ttl5;
DROP TABLE IF EXISTS test_fast_ttl6;
