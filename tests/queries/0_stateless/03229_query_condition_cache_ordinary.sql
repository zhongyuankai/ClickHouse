-- Tags: no-parallel, no-parallel-replicas

-- SET allow_experimental_analyzer = 1;

SET allow_deprecated_database_ordinary=1;
DROP DATABASE IF EXISTS db_03229_ordinary;
CREATE DATABASE db_03229_ordinary Engine=Ordinary;

SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE IF EXISTS db_03229_ordinary.tab1;
DROP TABLE IF EXISTS db_03229_ordinary.tab2;

CREATE TABLE db_03229_ordinary.tab1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO db_03229_ordinary.tab1 SELECT number, number FROM numbers(500000);
INSERT INTO db_03229_ordinary.tab1 SELECT number, number FROM numbers(500000);

CREATE TABLE db_03229_ordinary.tab2 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO db_03229_ordinary.tab2 SELECT number, 1000000 - number FROM numbers(1000000);

SELECT '====Test prewhere====';
SELECT '====SELECT count(*)====';
SELECT count(*) FROM db_03229_ordinary.tab1 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;
SELECT count(*) FROM db_03229_ordinary.tab2 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;

SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM db_03229_ordinary.tab1 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM db_03229_ordinary.tab2 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT '====SELECT *====';
SELECT * FROM db_03229_ordinary.tab1 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;
SELECT * FROM db_03229_ordinary.tab2 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;

SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM db_03229_ordinary.tab1 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;

SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM db_03229_ordinary.tab2 PREWHERE b = 10000 SETTINGS use_query_condition_cache = true;'
ORDER BY
    event_time_microseconds;


-- Now test without move to PREWHERE
SYSTEM DROP QUERY CONDITION CACHE;


SELECT '====Test where====';
SELECT '====SELECT count(*)====';
SELECT count(*) FROM db_03229_ordinary.tab1 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;
SELECT count(*) FROM db_03229_ordinary.tab2 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;

SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM db_03229_ordinary.tab1 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count(*) FROM db_03229_ordinary.tab2 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;


SELECT '====SELECT *====';
SELECT * FROM db_03229_ordinary.tab1 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;
SELECT * FROM db_03229_ordinary.tab2 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;

SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM db_03229_ordinary.tab1 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM db_03229_ordinary.tab2 WHERE b = 10000 SETTINGS use_query_condition_cache = true, optimize_move_to_prewhere = false;'
ORDER BY
    event_time_microseconds;

Select '====Test `in` operator====';
CREATE TABLE db_03229_ordinary.tab3 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO db_03229_ordinary.tab3 values (1, 10), (2, 20);
INSERT INTO db_03229_ordinary.tab3 values (10, 100), (20, 200);

SELECT * FROM db_03229_ordinary.tab3 WHERE b in (10, 20) SETTINGS use_query_condition_cache = true;
SELECT * FROM db_03229_ordinary.tab3 WHERE b in (100, 200) SETTINGS use_query_condition_cache = true;

DROP DATABASE IF EXISTS db_03229_ordinary;
