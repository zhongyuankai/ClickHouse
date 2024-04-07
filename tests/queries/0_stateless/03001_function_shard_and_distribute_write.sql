SELECT shard('test_cluster_two_shards');

DROP DATABASE IF EXISTS 03001_db;
CREATE DATABASE IF NOT EXISTS 03001_db;

CREATE TABLE 03001_db.test03001_tb_local (a UInt64, s String) ENGINE = MergeTree()  ORDER BY a;
CREATE TABLE 03001_db.test03001_tb AS 03001_db.test03001_tb_local ENGINE = Distributed(test_cluster_two_shards, 03001_db, test03001_tb_local, shard('test_cluster_two_shards'), 1);

INSERT INTO 03001_db.test03001_tb VALUES (1, '1.1'), (2, '2.2'), (3, '3.3');
INSERT INTO 03001_db.test03001_tb VALUES (4, '4.4'), (5, '5.5'), (6, '6.6');
INSERT INTO 03001_db.test03001_tb VALUES (7, '7.7'), (8, '8.8'), (9, '9.9');

SELECT * from 03001_db.test03001_tb_local order by a;

DROP DATABASE IF EXISTS 03001_db;
