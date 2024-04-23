SELECT 'Test MergeTree with ttl_only_drop_part set to false.';
DROP TABLE IF EXISTS test_fast_ttl;
CREATE TABLE test_fast_ttl (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = MergeTree()
PARTITION BY name
ORDER BY id
TTL create_time + toIntervalDay(300)
SETTINGS ttl_only_drop_parts = false;

INSERT INTO test_fast_ttl SELECT number, 'Anna', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', now() from numbers(2000);

SELECT 'Before modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl;
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 10 DAY;

SELECT 'After modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;

SELECT 'Test merge';
INSERT INTO test_fast_ttl SELECT number, 'Anna', if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl FINAL;
SELECT COUNT(*) FROM test_fast_ttl;


SELECT 'Test MergeTree with ttl_only_drop_part set to true.';
DROP TABLE IF EXISTS test_fast_ttl;
CREATE TABLE test_fast_ttl (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = MergeTree()
PARTITION BY name
ORDER BY id
TTL create_time + toIntervalDay(300)
SETTINGS ttl_only_drop_parts = true;

INSERT INTO test_fast_ttl SELECT number, 'Anna', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', now() from numbers(2000);

SELECT 'Before modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl;
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 10 DAY;

SELECT 'After modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;

SELECT 'Test merge';
INSERT INTO test_fast_ttl SELECT number, 'Anna',  if(number >= 1000, date_sub(day, 20, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl FINAL;
SELECT COUNT(*) FROM test_fast_ttl;


SELECT 'Test MergeTree without ttl.';
DROP TABLE IF EXISTS test_fast_ttl;
CREATE TABLE test_fast_ttl (`id` UInt32, `name` String, `create_time` DateTime)
ENGINE = MergeTree()
PARTITION BY name
ORDER BY id
SETTINGS ttl_only_drop_parts = false;

INSERT INTO test_fast_ttl SELECT number, 'Anna', date_sub(day, 100, now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', now() from numbers(2000);

SELECT 'Before modifying TTL';
SELECT COUNT(*) FROM test_fast_ttl;

SELECT 'Add modifying TTL';
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 300 DAY;
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;

SELECT 'Shorten TTL';
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 10 DAY;
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;

SELECT 'Extend TTL';
INSERT INTO test_fast_ttl SELECT number, 'jan', date_sub(day, 70, now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'jan', if(number >= 1000, date_sub(day, 50, now()), now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'Anna', date_sub(day, 5, now()) from numbers(2000);
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 40 DAY;
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;

SELECT 'Shorten TTL and ttl_only_drop_part set to true';
ALTER TABLE test_fast_ttl MODIFY SETTING ttl_only_drop_parts = true;
INSERT INTO test_fast_ttl SELECT number, 'jan', date_sub(day, 5, now()) from numbers(2000);
INSERT INTO test_fast_ttl SELECT number, 'Anna', if(number >= 1000, date_sub(day, 5, now()), now()) from numbers(2000);
ALTER TABLE test_fast_ttl MODIFY TTL create_time + INTERVAL 2 DAY;
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;

SELECT 'Test merge';
INSERT INTO test_fast_ttl SELECT number, 'Anna', if(number >= 1000, date_sub(day, 5, now()), now()) from numbers(2000);
OPTIMIZE TABLE test_fast_ttl FINAL;
SELECT COUNT(*) FROM test_fast_ttl;
SELECT id, name FROM test_fast_ttl ORDER BY name, id limit 3;


DROP TABLE IF EXISTS test_fast_ttl;
