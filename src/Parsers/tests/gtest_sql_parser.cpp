#include <gtest/gtest.h>
#include <Parsers/QueryParseTools.h>

TEST(SQLParser, ParseInsertQuery)
{
    String sql1 = "insert into db_stream.test_parser_tb (col1, col2) values(1,'a'),(2,'b')";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "INSERT;db_stream;test_parser_tb");

    String sql2 = "insert into test_parser_tb (col1, col2) values(1,'a'),(2,'b')";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "INSERT;;test_parser_tb");

    String sql3 = "insert into test_parser_tb select * from db_stream.test__tb limit 10";
    String res3 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res3, "INSERT;;test_parser_tb");
}

TEST(SQLParser, ParseSelectQuery)
{
    String sql1 = "select * from db_stream.test_parser_tb UNION ALL select * from test_tb";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "SELECT;db_stream;test_parser_tb");

    String sql2 = "select * from (select a,b,c from db_stream.test_parser_tb order by a limit 10)";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "SELECT;db_stream;test_parser_tb");

    String sql3 = "select * from (select a,b,c from test_parser_tb order by a limit 10) t1 join test_tb t2 on t1.a = t2.a";
    String res3 = DB::parseAnyDatabaseAndTable(sql3);
    ASSERT_EQ(res3, "SELECT;;test_parser_tb");

    String sql4 = "select a,b,c from test_parser_tb where a='xxx' group by a,b,c order by a limit 10";
    String res4 = DB::parseAnyDatabaseAndTable(sql4);
    ASSERT_EQ(res4, "SELECT;;test_parser_tb");

    String sql5 = "select row_number() OVER (PARTITION BY req_id ORDER BY call_time DESC) AS rk from db_stream.test_parser_tb";
    String res5 = DB::parseAnyDatabaseAndTable(sql5);
    ASSERT_EQ(res5, "SELECT;db_stream;test_parser_tb");
}

TEST(SQLParser, ParseDescQuery)
{
    String sql1 = "desc test_parser_tb";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "DESCRIBE;;test_parser_tb");

    String sql2 = "desc db_stream.test_parser_tb";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "DESCRIBE;db_stream;test_parser_tb");
}

TEST(SQLParser, ParseShowQuery)
{
    String sql1 = "show tables";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "SHOW;;");

    String sql2 = "show tables from db_stream";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "SHOW;db_stream;");
}

TEST(SQLParser, ParseExplainQuery)
{
    String sql1 = "explain select * from db_stream.test_parser_tb UNION ALL select * from test_tb";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "EXPLAIN;db_stream;test_parser_tb");

    String sql2 = "explain select * from (select a,b,c from db_stream.test_parser_tb order by a limit 10)";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "EXPLAIN;db_stream;test_parser_tb");

    String sql3 = "explain select * from (select a,b,c from test_parser_tb order by a limit 10) t1 join test_tb t2 on t1.a = t2.a";
    String res3 = DB::parseAnyDatabaseAndTable(sql3);
    ASSERT_EQ(res3, "EXPLAIN;;test_parser_tb");

    String sql4 = "explain select a,b,c from test_parser_tb where a='xxx' group by a,b,c order by a limit 10";
    String res4 = DB::parseAnyDatabaseAndTable(sql4);
    ASSERT_EQ(res4, "EXPLAIN;;test_parser_tb");
}

TEST(SQLParser, ParseAlterQuery)
{
    String sql1 = "alter table test_parser_tb replace partition 20240911 from db_stream.test_tb";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "ALTER;;test_parser_tb");

    String sql2 = "alter table db_stream.test_parser_tb replace partition 20240911 from test_tb";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "ALTER;db_stream;test_parser_tb");
}

TEST(SQLParser, ParseCreateQuery)
{
    String sql1 = "create table db_stream.test_parser_tb (a String) engine=MergeTree() order by a";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "CREATE;db_stream;test_parser_tb");

    String sql2 = "create materialized view test_parser_tb_view to db_stream.test_parser_tb_view (a String) as select a from test_tb";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2, "CREATE;;test_parser_tb_view");

    String sql3 = "create database db_stream";
    String res3 = DB::parseAnyDatabaseAndTable(sql3);
    ASSERT_EQ(res3, "CREATE;db_stream;");
}

TEST(SQLParser, ParseUnknowQuery)
{
    String sql1 = "system restart replica db_stream.test_parser_tb";
    String res1 = DB::parseAnyDatabaseAndTable(sql1);
    ASSERT_EQ(res1, "UNKNOWN;;");

    String sql2 = "select  from db_stream.test_parser_tb";
    String res2 = DB::parseAnyDatabaseAndTable(sql2);
    ASSERT_EQ(res2.substr(0,101), "Code: 62. DB::Exception: Syntax error: failed at position 14 ('db_stream'): db_stream.test_parser_tb.");
}
