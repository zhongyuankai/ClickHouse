-- Tags: no-fasttest

SELECT 'Test diMd5 function';
SELECT diMd5('admin');
SELECT diMd5('');

DROP TABLE IF EXISTS test_di_md5;
CREATE TABLE test_di_md5 (s String) ENGINE = Memory;
INSERT INTO test_di_md5 SELECT concat('1234567890_', toString(number)) FROM numbers(10000);

SELECT '======original text=====';
SELECT s FROM test_di_md5 ORDER BY s LIMIT 10;

SELECT '======di md5=====';
SELECT diMd5(s) s FROM test_di_md5 ORDER BY s LIMIT 10;

DROP TABLE IF EXISTS test_di_md5;
