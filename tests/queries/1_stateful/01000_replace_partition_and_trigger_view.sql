DROP TABLE IF EXISTS test.visits_basic;
DROP TABLE IF EXISTS test.visits_basic_tmp;
DROP TABLE IF EXISTS test.visits_trigger_view_00;
DROP TABLE IF EXISTS test.visits_trigger_view_01;

CREATE TABLE test.visits_basic (StartURL String, Duration UInt32, StartDate Date ) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(StartDate) ORDER BY StartURL;

CREATE TABLE test.visits_basic_tmp (StartURL String,Duration UInt32,StartDate Date ) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(StartDate) ORDER BY StartURL;

CREATE MATERIALIZED VIEW test.visits_trigger_view_00 ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMMDD(StartDate) ORDER BY url AS SELECT StartURL AS url, avg(Duration) AS avg_duration, StartDate FROM test.visits_basic GROUP BY url, StartDate;

INSERT INTO test.visits_basic_tmp SELECT StartURL, Duration, StartDate FROM test.visits WHERE StartDate >= '2014-03-17' AND StartDate <= '2014-03-23';

ALTER TABLE test.visits_basic REPLACE PARTITION '20140317' FROM test.visits_basic_tmp AND TRIGGER VIEW;

SELECT avg_duration FROM test.visits_trigger_view_00 ORDER BY avg_duration DESC limit 10;

ALTER TABLE test.visits_basic REPLACE PARTITION '20140318' FROM test.visits_basic_tmp;

CREATE MATERIALIZED VIEW test.visits_trigger_view_01 ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMMDD(StartDate) ORDER BY url AS SELECT StartURL AS url, avg(Duration) AS avg_duration, StartDate FROM test.visits_basic GROUP BY url, StartDate;

ALTER TABLE test.visits_basic REPLACE PARTITION '20140319' FROM test.visits_basic_tmp, REPLACE PARTITION '20140320' FROM test.visits_basic_tmp AND TRIGGER VIEW SETTINGS max_bytes_before_external_group_by = 10000000000, max_memory_usage=19000000000, max_insert_threads=5, max_execution_time=60000, lock_acquire_timeout=86400000;

SELECT '======select view_00======';

SELECT StartDate, avg_duration FROM test.visits_trigger_view_00 ORDER BY StartDate, avg_duration DESC limit 10;

SELECT '======select view_01======';

SELECT StartDate, avg_duration FROM test.visits_trigger_view_01 ORDER BY StartDate, avg_duration DESC limit 10;

SELECT '======select basic======';

SELECT StartDate, avg_duration FROM (SELECT StartURL AS url, avg(Duration) AS avg_duration, StartDate FROM test.visits_basic GROUP BY url, StartDate) ORDER BY StartDate, avg_duration DESC limit 10;


ALTER TABLE test.visits_basic REPLACE PARTITION '20140321' FROM test.visits_basic_tmp, REPLACE PARTITION '20140322' FROM test.visits_basic_tmp;

SELECT '======select view_00 StartDate======';

SELECT StartDate from test.visits_trigger_view_00 GROUP BY StartDate;

SELECT '======select view_01 StartDate======';

SELECT StartDate from test.visits_trigger_view_01 GROUP BY StartDate;

SELECT '======select basic StartDate======';

SELECT StartDate from test.visits_basic GROUP BY StartDate;

DROP TABLE test.visits_basic;
DROP TABLE test.visits_basic_tmp;
DROP TABLE test.visits_trigger_view_00;
DROP TABLE test.visits_trigger_view_01;
