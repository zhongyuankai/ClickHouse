-- Tags: no-parallel

DROP TABLE IF EXISTS snapshot_op;
CREATE TABLE snapshot_op(i int,  create_time DateTime) ENGINE MergeTree PARTITION BY toYYYYMMDD(create_time) ORDER BY i;

SELECT 'Test MergeTree snapshot';
INSERT INTO snapshot_op VALUES (1, toDateTime('2020-09-01 00:00:00')), (2, toDateTime('2020-09-02 00:00:00'));
SELECT 'Add snapshot';
ALTER TABLE snapshot_op ADD SNAPSHOT;
INSERT INTO snapshot_op VALUES (3, toDateTime('2020-09-02 00:00:00'));

SELECT COUNT(*) FROM snapshot_op;
SELECT COUNT(*) FROM snapshot_op SNAPSHOT;
SELECT COUNT(*) FROM snapshot_op SETTINGS read_experimental_snapshot=1;

SELECT 'Drop snapshot';
ALTER TABLE snapshot_op DROP SNAPSHOT;
select COUNT(*) FROM snapshot_op SNAPSHOT;

SELECT 'Create materialized view and add snapshot';
DROP TABLE IF EXISTS snapshot_op_mv;
CREATE MATERIALIZED VIEW snapshot_op_mv ENGINE = MergeTree ORDER BY (i) SNAPSHOT AS SELECT i, create_time FROM snapshot_op;
INSERT INTO snapshot_op VALUES (4, toDateTime('2020-09-04 00:00:00'));

SELECT COUNT(*) FROM snapshot_op;
SELECT COUNT(*) FROM snapshot_op SNAPSHOT;

SELECT 'Test merge';
INSERT INTO snapshot_op VALUES (5, toDateTime('2020-09-01 00:00:00'));
INSERT INTO snapshot_op VALUES (6, toDateTime('2020-09-01 00:00:00'));
INSERT INTO snapshot_op VALUES (7, toDateTime('2020-09-01 00:00:00'));
INSERT INTO snapshot_op VALUES (8, toDateTime('2020-09-01 00:00:00'));
INSERT INTO snapshot_op VALUES (9, toDateTime('2020-09-01 00:00:00'));

OPTIMIZE TABLE snapshot_op;
SELECT COUNT(*) FROM snapshot_op;
SELECT COUNT(*) FROM snapshot_op SNAPSHOT;

SELECT 'Drop base table snapshot';
ALTER TABLE snapshot_op DROP SNAPSHOT;
SELECT COUNT(*) FROM snapshot_op SNAPSHOT;

SELECT 'Test `allow_locktable_oncreate` setting';
DROP TABLE IF EXISTS snapshot_op_mv;
CREATE MATERIALIZED VIEW snapshot_op_mv ENGINE = MergeTree ORDER BY (i) SNAPSHOT AS SELECT i, create_time FROM snapshot_op SETTINGS allow_locktable_oncreate = 0;
INSERT INTO snapshot_op VALUES (5, toDateTime('2020-09-05 00:00:00'));

SELECT COUNT(*) FROM snapshot_op;
SELECT COUNT(*) FROM snapshot_op SNAPSHOT;
DROP TABLE IF EXISTS snapshot_op;
DROP TABLE IF EXISTS snapshot_op_mv;

SELECT 'Test ReplicatedMergeTree snapshot';
SET alter_sync = 2;

DROP TABLE IF EXISTS replica1_snapshot_op;
DROP TABLE IF EXISTS replica2_snapshot_op;
CREATE TABLE IF NOT EXISTS replica1_snapshot_op (i int,  create_time DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/replica_snapshot_op', '1')
PARTITION BY toYYYYMMDD(create_time) ORDER BY i;

CREATE TABLE IF NOT EXISTS replica2_snapshot_op (i int,  create_time DateTime)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/replica_snapshot_op', '2')
PARTITION BY toYYYYMMDD(create_time) ORDER BY i;

SELECT 'Add snapshot';
ALTER TABLE replica1_snapshot_op ADD SNAPSHOT;
SELECT sleep(3);
INSERT INTO replica1_snapshot_op VALUES (1, toDateTime('2020-09-01 00:00:00')), (2, toDateTime('2020-09-02 00:00:00'));
SYSTEM SYNC REPLICA replica2_snapshot_op;

SELECT COUNT(*) FROM replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;
SELECT COUNT(*) FROM replica2_snapshot_op  SETTINGS read_experimental_snapshot = 1;

SELECT 'Drop snapshot';
ALTER TABLE replica1_snapshot_op DROP SNAPSHOT;
SELECT sleep(3);
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;

SELECT 'Add snapshot again';
ALTER TABLE replica1_snapshot_op ADD SNAPSHOT;
INSERT INTO replica1_snapshot_op VALUES (3, toDateTime('2020-09-03 00:00:00'));
SYSTEM SYNC REPLICA replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;
SELECT COUNT(*) FROM replica2_snapshot_op  SETTINGS read_experimental_snapshot=1;

SELECT 'Drop snapshot again';
ALTER TABLE replica1_snapshot_op DROP SNAPSHOT;
ALTER TABLE replica2_snapshot_op DROP SNAPSHOT;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;

SELECT 'Create materialized view and add snapshot';
DROP TABLE IF EXISTS replica1_snapshot_op_mv;
DROP TABLE IF EXISTS replica2_snapshot_op_mv;
CREATE MATERIALIZED VIEW replica1_snapshot_op_mv
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/replica_snapshot_op_mv', '1')
ORDER BY (i) SNAPSHOT AS SELECT i, create_time FROM replica1_snapshot_op;

CREATE MATERIALIZED VIEW replica2_snapshot_op_mv
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/replica_snapshot_op_mv', '2')
ORDER BY (i) SNAPSHOT AS SELECT i, create_time FROM replica2_snapshot_op;

INSERT INTO replica1_snapshot_op VALUES (4, toDateTime('2020-09-04 00:00:00'));
SELECT COUNT(*) FROM replica1_snapshot_op_mv;

SYSTEM SYNC REPLICA replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;

SELECT 'Test merge';
INSERT INTO replica1_snapshot_op VALUES (5, toDateTime('2020-09-01 00:00:00'));
INSERT INTO replica1_snapshot_op VALUES (6, toDateTime('2020-09-01 00:00:00'));
INSERT INTO replica1_snapshot_op VALUES (7, toDateTime('2020-09-01 00:00:00'));
INSERT INTO replica1_snapshot_op VALUES (8, toDateTime('2020-09-01 00:00:00'));
INSERT INTO replica1_snapshot_op VALUES (9, toDateTime('2020-09-01 00:00:00'));

OPTIMIZE TABLE replica1_snapshot_op;
SYSTEM SYNC REPLICA replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;

SELECT 'Drop base table snapshot';
ALTER TABLE replica1_snapshot_op DROP SNAPSHOT;
ALTER TABLE replica2_snapshot_op DROP SNAPSHOT;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;

SELECT 'Test `allow_locktable_oncreate` setting';
DROP TABLE IF EXISTS replica3_snapshot_op_mv;
DROP TABLE IF EXISTS replica4_snapshot_op_mv;
CREATE MATERIALIZED VIEW replica3_snapshot_op_mv
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/replica3_snapshot_op_mv', '1')
ORDER BY (i) SNAPSHOT AS SELECT i, create_time FROM replica1_snapshot_op
SETTINGS allow_locktable_oncreate = 0;

CREATE MATERIALIZED VIEW replica4_snapshot_op_mv
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/replica3_snapshot_op_mv', '2')
ORDER BY (i) SNAPSHOT AS SELECT i, create_time FROM replica2_snapshot_op
SETTINGS allow_locktable_oncreate = 0;

INSERT INTO replica1_snapshot_op VALUES (5, toDateTime('2020-09-05 00:00:00'));
SELECT COUNT(*) FROM replica3_snapshot_op_mv;

SYSTEM SYNC REPLICA replica2_snapshot_op;
SELECT COUNT(*) FROM replica2_snapshot_op SNAPSHOT;
SELECT COUNT(*) FROM replica2_snapshot_op;

DROP TABLE IF EXISTS replica1_snapshot_op;
DROP TABLE IF EXISTS replica2_snapshot_op;
DROP TABLE IF EXISTS replica1_snapshot_op_mv;
DROP TABLE IF EXISTS replica2_snapshot_op_mv;
DROP TABLE IF EXISTS replica3_snapshot_op_mv;
