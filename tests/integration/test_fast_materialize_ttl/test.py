import pytest
import time
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/user_settings.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/user_settings.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 2},
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for _, node in cluster.instances.items():
            node.query(
                """
                CREATE TABLE test_tb (c1 UInt32, c2 UInt32, c3 String, p UInt32, _sys_insert_time DateTime)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_tb', '{replica}')
                PARTITION BY p ORDER BY c2 TTL _sys_insert_time + toIntervalDay(300)
                SETTINGS ttl_only_drop_parts = true, merge_with_ttl_timeout = 0;
                CREATE TABLE test_compact_tb (c1 UInt32, c2 UInt32, c3 String, p UInt32, _sys_insert_time DateTime)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_compact_tb', '{replica}')
                PARTITION BY p ORDER BY c2 TTL _sys_insert_time + toIntervalDay(300)
                SETTINGS ttl_only_drop_parts = true, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 107374182400;
                """
            )

        yield cluster

    finally:
        cluster.shutdown()


def modify_ttl(table_name):
    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 1 p, date_sub(day, 20 + (number % 80), now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 2 p, date_sub(day, 5 + (number % 10), now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 3 p, now() _sys_insert_time FROM numbers(20000000);"""
    )

    node1.query(
        f"""ALTER TABLE {table_name} MODIFY TTL _sys_insert_time + toIntervalDay(10);"""
    )

    node2.query(f"""SYSTEM SYNC REPLICA {table_name};""")
    assert int(node2.query(f"""SELECT count(*) FROM {table_name};""")) <= 40000000

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 1 p, date_sub(day, 20, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 2 p, date_sub(day, 5, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node2.query(f"""SYSTEM SYNC REPLICA {table_name};""")
    time.sleep(5)
    node2.query(f"""OPTIMIZE TABLE {table_name};""")
    count = int(node2.query(f"""SELECT count(*) FROM {table_name};"""))
    assert count <= 60000000
    assert count > 40000000

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    assert (
        int(
            node1.query(
                f"""SELECT max(c2) FROM {table_name} WHERE _sys_insert_time > date_sub(day, 100, now());"""
            )
        )
        == 19999999
    )
    assert (
        int(
            node2.query(
                f"""SELECT max(c2) FROM {table_name} WHERE _sys_insert_time > date_sub(day, 100, now());"""
            )
        )
        == 19999999
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 1 p, date_sub(day, 12, now()) _sys_insert_time FROM numbers(20000000);"""
    )
    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 3 p, date_sub(day, 6, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node2.query(f"""SYSTEM SYNC REPLICA {table_name};""")
    time.sleep(5)
    node2.query(f"""OPTIMIZE TABLE {table_name};""")
    count = int(node2.query(f"""SELECT count(*) FROM {table_name};"""))
    assert count <= 80000000
    assert count > 60000000

    node1.query(
        f"""ALTER TABLE {table_name} MODIFY TTL _sys_insert_time + toIntervalDay(14);"""
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 1 p, date_sub(day, 30, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 2 p, date_sub(day, 12, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node2.query(f"""SYSTEM SYNC REPLICA {table_name};""")
    time.sleep(5)
    node2.query(f"""OPTIMIZE TABLE {table_name};""")
    count = int(node2.query(f"""SELECT count(*) FROM {table_name};"""))
    assert count <= 100000000
    assert count > 80000000

    node1.restart_clickhouse()
    node2.restart_clickhouse()

    assert (
        int(
            node1.query(
                f"""SELECT max(c2) FROM {table_name} WHERE _sys_insert_time > date_sub(day, 10, now());"""
            )
        )
        == 19999999
    )
    assert (
        int(
            node2.query(
                f"""SELECT max(c2) FROM {table_name} WHERE _sys_insert_time > date_sub(day, 10, now());"""
            )
        )
        == 19999999
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 1 p, date_sub(day, 22, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node1.query(
        f"""INSERT INTO {table_name} SELECT toUInt32(rand()) c1, number c2, toString(rand()) c3, 3 p, date_sub(day, 13, now()) _sys_insert_time FROM numbers(20000000);"""
    )

    node2.query(f"""SYSTEM SYNC REPLICA {table_name};""")
    time.sleep(5)
    node2.query(f"""OPTIMIZE TABLE {table_name};""")
    count = int(node2.query(f"""SELECT count(*) FROM {table_name};"""))
    assert count <= 120000000
    assert count > 100000000

    node1.query(f"""DROP TABLE {table_name};""")
    node2.query(f"""DROP TABLE {table_name};""")


def test_fast_materialize_ttl(started_cluster):
    modify_ttl("test_tb")
    modify_ttl("test_compact_tb")
