-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

DROP TABLE IF EXISTS test_expired_column_tb;
CREATE TABLE test_expired_column_tb
(
    id UInt64,
    logTimeHour DateTime
)
    ENGINE = MergeTree
ORDER BY logTimeHour;

insert into test_expired_column_tb select number,'2023-10-26 12:00:00' from system.numbers limit 10;
insert into test_expired_column_tb select number,'2023-10-26 13:00:00' from system.numbers limit 10,10;
insert into test_expired_column_tb select number,'2023-10-26 14:00:00' from system.numbers limit 20,10;
insert into test_expired_column_tb select number,'2023-10-26 15:00:00' from system.numbers limit 30,10;
insert into test_expired_column_tb select number,toStartOfHour(now()) from system.numbers limit 40,10;
insert into test_expired_column_tb select number,date_sub(hour, 1, toStartOfHour(now())) from system.numbers limit 50,10;
insert into test_expired_column_tb select number,date_sub(hour, -1, toStartOfHour(now())) from system.numbers limit 60,10;

set query_cache_expired_data_in_hour=3;
set query_cache_ttl=60;
set query_cache_ttl_for_expired_data=240;
set query_cache_store_results_of_queries_with_nondeterministic_functions=true;

SYSTEM DROP QUERY CACHE;
select id from test_expired_column_tb where logTimeHour=toStartOfHour(now()) order by id limit 10 settings use_query_cache=true, query_cache_expired_column='logTimeHour';
select (expires_at - now()) > 100 from system.query_cache;

select '=============';
SYSTEM DROP QUERY CACHE;
select * from test_expired_column_tb where logTimeHour='2023-10-26 15:00:00' order by id limit 10 settings use_query_cache=true, query_cache_expired_column='logTimeHour';
select (expires_at - now()) > 100 from system.query_cache;

select '=============';
SYSTEM DROP QUERY CACHE;
select id from test_expired_column_tb where id=1 limit 10 settings use_query_cache=true, query_cache_expired_column='logTimeHour';
select (expires_at - now()) > 100 from system.query_cache;

select '=============';
SYSTEM DROP QUERY CACHE;
select count(*) from test_expired_column_tb settings use_query_cache=true, query_cache_expired_column='logTimeHour';
select (expires_at - now()) > 100 from system.query_cache;

select '=============';
SYSTEM DROP QUERY CACHE;
select * from test_expired_column_tb where logTimeHour='2023-10-26 15:00:00' settings use_query_cache=true, query_cache_expired_column='xxx';
select (expires_at - now()) > 100 from system.query_cache;

DROP TABLE IF EXISTS test_expired_column_tb;
