#include <gtest/gtest.h>
#include <Parsers/QueryParseTools.h>

TEST(SQLClassifier, SelectQuery)
{
    std::string sql1 = "SELECT grid_id,     coalesce(COUNT(DISTINCT user_id), 0) as pay_user_cnt,     coalesce(sum(gmv), 0) as gmv,     coalesce(sum(if(biz_type = '1', pay_amount, 0)), 0) as actual_gmv_amt,     coalesce(COUNT(DISTINCT leader_uid), 0) as pay_led_cnt FROM wujie_stream.dwd_cx_trd_order_leader_base_rt_view WHERE (status in (1, 3, 4, 5, 7, 6, 15, 16))     AND (         create_time >= '2021-08-01 00:00:00'         AND create_time < '2021-08-01 20:15:00'     )     AND (channel IN ('1'))     AND (         grid_id IN (             '9f30c7e20333993dc4b63680a0aade97',             '71abae9dab55764ab87f0183e4dfcd25',             '25b808f8a79f7f05c28d23cae11f0d07',             '00bdd8df082a44cb4721c0f0d853f830',             '4aa6f1a255715961ff7dcf17536a8029',             '1bd47562e2ff0be20d8c123b90b242e2',             'f0c6f97a3c136a9ff4569461f73166c4',             'dba946ae9e709dde31b20471b712c06f',             '30475287926237a12ab979ffe23ec17c',             '2bdcb3cf2db80746ff53df0836035856',             'c27380df4630170adddb79b5312e5c1c',             'f0860ad456e2e22489fcd97a3da37198',             '9657f3cbfc894882935f6fa86c9ae6b9',             'd70e3898707f6a25ffc40128f3d8235a'         )     ) GROUP BY grid_id FORMAT TabSeparatedWithNamesAndTypes;";
    String res1 = DB::outputQueryClassId(sql1);
    ASSERT_EQ(res1, "16661318463605495552");

    std::string sql2 = "SELECT grid_id,     coalesce(COUNT(DISTINCT user_id), 0) as pay_user_cnt,     coalesce(sum(gmv), 0) as gmv,     coalesce(sum(if(biz_type = '1', pay_amount, 0)), 0) as actual_gmv_amt,     coalesce(COUNT(DISTINCT leader_uid), 0) as pay_led_cnt FROM wujie_stream.dwd_cx_trd_order_leader_base_rt_view WHERE (status in (6, 15, 16))     AND (         create_time >= '2021-08-01 00:00:00'         AND create_time < '2021-08-01 20:15:00'     )     AND (channel IN ('1'))     AND (         grid_id IN (             '9f30c7e20333993dc4b63680a0aade97',             '71abae9dab55764ab87f0183e4dfcd25',             '25b808f8a79f7f05c28d23cae11f0d07',             '00bdd8df082a44cb4721c0f0d853f830',             '4aa6f1a255715961ff7dcf17536a8029',             '1bd47562e2ff0be20d8c123b90b242e2',             'f0c6f97a3c136a9ff4569461f73166c4',             'dba946ae9e709dde31b20471b712c06f',             '30475287926237a12ab979ffe23ec17c',             '2bdcb3cf2db80746ff53df0836035856',             'c27380df4630170adddb79b5312e5c1c',             'f0860ad456e2e22489fcd97a3da37198',             '9657f3cbfc894882935f6fa86c9ae6b9',             'd70e3898707f6a25ffc40128f3d8235a'         )     ) GROUP BY grid_id FORMAT TabSeparatedWithNamesAndTypes;";
    String res2 = DB::outputQueryClassId(sql2);
    ASSERT_EQ(res2, "16661318463604046912");

    String sql3 = "select * from (select a,b,c from test_parser_tb order by a limit 10) t1 join test_tb t2 on t1.a = t2.a";
    String res3 = DB::outputQueryClassId(sql3);
    ASSERT_EQ(res3, "11647785727172804608");
}

