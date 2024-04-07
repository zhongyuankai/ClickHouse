import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query(
                """CREATE TABLE local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id;"""
            )

        node1.query(
            """CREATE TABLE distributed_table(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table, shard('test_cluster'), 1);"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_write_distributed_only_write_local(started_cluster):
    node1.query("INSERT INTO distributed_table VALUES (0, 'node1')")
    node1.query("INSERT INTO distributed_table VALUES (1, 'node2')")
    node1.query("INSERT INTO distributed_table VALUES (2, 'node1')")
    node1.query("INSERT INTO distributed_table VALUES (3, 'node2')")
    node1.query("INSERT INTO distributed_table VALUES (4, 'node1')")
    node1.query("INSERT INTO distributed_table VALUES (5, 'node2')")
    node1.query("INSERT INTO distributed_table VALUES (6, 'node1')")
    node1.query("INSERT INTO distributed_table VALUES (7, 'node2')")

    # Write only to this node when distributed directly to local
    assert node1.query("SELECT COUNT() FROM local_table").rstrip() == "8"

    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table;")
    assert node2.query("SELECT COUNT() FROM local_table").rstrip() == "0"
