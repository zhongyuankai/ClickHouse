import time
import pytest
import threading
import io
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 2},
)

node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)

node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def generate_data(node, table, i):
    response = TSV.toMat(
        node.query(
            "SELECT toUInt32(rand() / 10) c1, number c2, '"
            + str(i)
            + "' s, number % 10 v FROM numbers(20000) FORMAT TSVWithNames"
        )
    )

    c1_idx = response[0].index("c1")
    c2_idx = response[0].index("c2")
    s_idx = response[0].index("s")
    v_idx = response[0].index("v")

    rows = len(response)

    buffer = io.StringIO()
    buffer.write("INSERT into " + table + " values")
    for index, fields in enumerate(response[1:], start=1):  # skip header
        buffer.write("(")
        buffer.write(fields[c1_idx])
        buffer.write(",")
        buffer.write(fields[c2_idx])
        buffer.write(",'")
        buffer.write(fields[s_idx])
        buffer.write("',")
        buffer.write(fields[v_idx])
        buffer.write(")")
        if index != rows - 1:
            buffer.write(",")

    return buffer.getvalue()


def execute_query(node, sql, retry=3):
    for i in range(retry):
        try:
            node.query(sql)
            break
        except Exception as e:
            if i == retry - 1:
                print("Exception:", e)
                raise
            time.sleep(1)


def do_insert(nodes, table, throw=True, nums=400):
    for i in range(nums):
        try:
            for j in range(3):
                try:
                    data = generate_data(nodes[(i + j) % 2], table + "_local", i)
                    nodes[(i + j) % 2].query(data)
                    break
                except Exception as e1:
                    print("Exception:", e1)
                    time.sleep(1)
                    if throw:
                        raise
        except Exception as e:
            print("Exception:", e)
            if throw:
                raise

    for node in nodes:
        execute_query(node, "SYSTEM FLUSH DISTRIBUTED " + table + "_view_dist")


def restart_node(table):
    for _ in range(25):
        for _, node in cluster.instances.items():
            try:
                if (
                    node.query(
                        "SELECT is_leader FROM system.replicas WHERE table = '"
                        + table
                        + "_view_local'"
                    )
                    == 1
                ):
                    node.restart_clickhouse()
                    time.sleep(10)
            except Exception as e:
                print("Exception:", e)


@pytest.mark.timeout(1200)
def test_replicated_unique_merge_tree(started_cluster):
    for _, node in cluster.instances.items():
        node.query(
            """
            CREATE TABLE test_local (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_local', '{replica}') ORDER BY c2;
            CREATE TABLE test_dist (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = Distributed(test_cluster, currentDatabase(), test_local, intHash32(c1));
            CREATE TABLE test_view_local (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{shard}/test_view_local', '{replica}', c1, v) ORDER BY c2;
            CREATE TABLE test_view_dist (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = Distributed(test_cluster, currentDatabase(), test_view_local, intHash32(c1));
            CREATE MATERIALIZED VIEW test_view_trigger TO test_view_dist AS SELECT * FROM test_local;
            """
        )

    insert_thread1 = threading.Thread(
        target=do_insert,
        args=(
            [node1, node2],
            "test",
        ),
    )
    insert_thread2 = threading.Thread(
        target=do_insert,
        args=(
            [node1, node2],
            "test",
        ),
    )
    insert_thread3 = threading.Thread(
        target=do_insert,
        args=(
            [node3, node4],
            "test",
        ),
    )
    insert_thread4 = threading.Thread(
        target=do_insert,
        args=(
            [node3, node4],
            "test",
        ),
    )

    insert_thread1.start()
    insert_thread2.start()
    insert_thread3.start()
    insert_thread4.start()

    insert_thread1.join()
    insert_thread2.join()
    insert_thread3.join()
    insert_thread4.join()

    for _, node in cluster.instances.items():
        node.query("SYSTEM SYNC REPLICA test_local")
        node.query("SYSTEM SYNC REPLICA test_view_local")

    count1 = int(node1.query("SELECT count(distinct c1) FROM test_dist"))
    count2 = int(node1.query("SELECT count(c1) FROM test_view_dist"))
    assert count1 == count2

    sum1 = int(node1.query("SELECT sum(DISTINCT c1) FROM test_dist"))
    sum2 = int(node1.query("SELECT sum(c1) FROM test_view_dist"))
    assert sum1 == sum2

    v1 = int(
        node1.query(
            "SELECT sum(mv) FROM (SELECT c1, max(v) mv FROM test_dist GROUP BY c1) t"
        )
    )
    v2 = int(node1.query("SELECT sum(v) FROM test_view_dist"))
    assert v1 == v2

    for _, node in cluster.instances.items():
        node.query(
            """
            DROP TABLE IF EXISTS test_local;
            DROP TABLE IF EXISTS test_dist;
            DROP TABLE IF EXISTS test_view_local;
            DROP TABLE IF EXISTS test_view_dist;
            DROP TABLE IF EXISTS test_view_trigger;
            """
        )


@pytest.mark.timeout(1200)
def test_replicated_unique_merge_tree_with_failure(started_cluster):
    for _, node in cluster.instances.items():
        node.query(
            """
            CREATE TABLE test_with_failure_local (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_with_failure_local', '{replica}') ORDER BY c2;
            CREATE TABLE test_with_failure_dist (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = Distributed(test_cluster, currentDatabase(), test_with_failure_local, intHash32(c1));
            CREATE TABLE test_with_failure_view_local (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{shard}/test_with_failure_view_local', '{replica}', c1, v) ORDER BY c2;
            CREATE TABLE test_with_failure_view_dist (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = Distributed(test_cluster, currentDatabase(), test_with_failure_view_local, intHash32(c1));
            CREATE MATERIALIZED VIEW test_with_failure_view_trigger TO test_with_failure_view_dist AS SELECT * FROM test_with_failure_local;
            """
        )

    insert_thread1 = threading.Thread(
        target=do_insert,
        args=(
            [node1, node2],
            "test_with_failure",
            False,
        ),
    )
    insert_thread2 = threading.Thread(
        target=do_insert,
        args=(
            [node1, node2],
            "test_with_failure",
            False,
        ),
    )
    insert_thread3 = threading.Thread(
        target=do_insert,
        args=(
            [node3, node4],
            "test_with_failure",
            False,
        ),
    )
    insert_thread4 = threading.Thread(
        target=do_insert,
        args=(
            [node3, node4],
            "test_with_failure",
            False,
        ),
    )
    restart_thread = threading.Thread(target=restart_node, args=("test_with_failure",))

    insert_thread1.start()
    insert_thread2.start()
    insert_thread3.start()
    insert_thread4.start()
    restart_thread.start()

    insert_thread1.join()
    insert_thread2.join()
    insert_thread3.join()
    insert_thread4.join()
    restart_thread.join()

    for _, node in cluster.instances.items():
        node.query("SYSTEM SYNC REPLICA test_with_failure_local")
        node.query("SYSTEM SYNC REPLICA test_with_failure_view_local")

    count1 = int(node1.query("SELECT count(distinct c1) FROM test_with_failure_dist"))
    count2 = int(node1.query("SELECT count(c1) FROM test_with_failure_view_dist"))
    assert count1 == count2

    sum1 = int(node1.query("SELECT sum(DISTINCT c1) FROM test_with_failure_dist"))
    sum2 = int(node1.query("SELECT sum(c1) FROM test_with_failure_view_dist"))
    assert sum1 == sum2

    v1 = int(
        node1.query(
            "SELECT sum(mv) FROM (SELECT c1, max(v) mv FROM test_with_failure_dist GROUP BY c1) t"
        )
    )
    v2 = int(node1.query("SELECT sum(v) FROM test_with_failure_view_dist"))
    assert v1 == v2

    for _, node in cluster.instances.items():
        node.query(
            """
            DROP TABLE IF EXISTS test_with_failure_local;
            DROP TABLE IF EXISTS test_with_failure_dist;
            DROP TABLE IF EXISTS test_with_failure_view_local;
            DROP TABLE IF EXISTS test_with_failure_view_dist;
            DROP TABLE IF EXISTS test_with_failure_view_trigger;
            """
        )


@pytest.mark.timeout(1200)
def test_replicated_unique_merge_tree_with_unique_log(started_cluster):
    for _, node in cluster.instances.items():
        node.query(
            """
            CREATE TABLE test_with_log_local (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_with_log_local', '{replica}') ORDER BY c2;
            CREATE TABLE test_with_log_dist (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = Distributed(test_cluster, currentDatabase(), test_with_log_local, intHash32(c1));
            CREATE TABLE test_with_log_view_local (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{shard}/test_with_log_view_local', '{replica}', c1, v) ORDER BY c2 SETTINGS write_unique_id_log=true, write_unique_id_log=true;
            CREATE TABLE test_with_log_view_dist (c1 UInt32, c2 UInt32, s String, v UInt32) ENGINE = Distributed(test_cluster, currentDatabase(), test_with_log_view_local, intHash32(c1));
            CREATE MATERIALIZED VIEW test_with_log_view_trigger TO test_with_log_view_dist AS SELECT * FROM test_with_log_local;
            """
        )

    insert_thread1 = threading.Thread(
        target=do_insert,
        args=(
            [node1, node2],
            "test_with_log",
            False,
        ),
    )
    insert_thread2 = threading.Thread(
        target=do_insert,
        args=(
            [node1, node2],
            "test_with_log",
            False,
        ),
    )
    insert_thread3 = threading.Thread(
        target=do_insert,
        args=(
            [node3, node4],
            "test_with_log",
            False,
        ),
    )
    insert_thread4 = threading.Thread(
        target=do_insert,
        args=(
            [node3, node4],
            "test_with_log",
            False,
        ),
    )
    restart_thread = threading.Thread(target=restart_node, args=("test_with_log",))

    insert_thread1.start()
    insert_thread2.start()
    insert_thread3.start()
    insert_thread4.start()
    restart_thread.start()

    insert_thread1.join()
    insert_thread2.join()
    insert_thread3.join()
    insert_thread4.join()
    restart_thread.join()

    for _, node in cluster.instances.items():
        node.query("SYSTEM SYNC REPLICA test_with_log_local")
        node.query("SYSTEM SYNC REPLICA test_with_log_view_local")

    count1 = int(node1.query("SELECT count(distinct c1) FROM test_with_log_dist"))
    count2 = int(node1.query("SELECT count(c1) FROM test_with_log_view_dist"))
    assert count1 == count2

    sum1 = int(node1.query("SELECT sum(DISTINCT c1) FROM test_with_log_dist"))
    sum2 = int(node1.query("SELECT sum(c1) FROM test_with_log_view_dist"))
    assert sum1 == sum2

    v1 = int(
        node1.query(
            "SELECT sum(mv) FROM (SELECT c1, max(v) mv FROM test_with_log_dist GROUP BY c1) t"
        )
    )
    v2 = int(node1.query("SELECT sum(v) FROM test_with_log_view_dist"))
    assert v1 == v2

    for _, node in cluster.instances.items():
        node.query(
            """
            DROP TABLE IF EXISTS test_with_log_local;
            DROP TABLE IF EXISTS test_with_log_dist;
            DROP TABLE IF EXISTS test_with_log_view_local;
            DROP TABLE IF EXISTS test_with_log_view_dist;
            DROP TABLE IF EXISTS test_with_log_view_trigger;
            """
        )
