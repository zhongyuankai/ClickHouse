import json
import random
import re
import string
import threading
import time
from multiprocessing.dummy import Pool

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/storage_configuration.xml",
    ],
    stay_alive=True,
    tmpfs=["/jbod1:size=100M", "/jbod2:size=100M", "/jbod3:size=100M"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_disk_broken(start_cluster):
    try:
        node.query(
            """
            CREATE TABLE tbl (p UInt8, d String)
            ENGINE = MergeTree()
            PARTITION BY p
            ORDER BY tuple()
            SETTINGS
                storage_policy = 'jbod',
                old_parts_lifetime = 1,
                cleanup_delay_period = 1,
                cleanup_delay_period_random_add = 2,
                cleanup_thread_preferred_points_per_iteration=0,
                max_bytes_to_merge_at_max_space_in_pool = 4096,
                skip_broken_part = true
        """
        )

        for i in range(50):
            # around 1k per block
            node.query("insert into tbl select number, 'A' from numbers(50)")

        node.query(
            """
            CREATE TABLE tbl2 (p UInt8, d String)
            ENGINE = MergeTree()
            ORDER BY p
            SETTINGS
                storage_policy = 'jbod',
                old_parts_lifetime = 1,
                cleanup_delay_period = 1,
                cleanup_delay_period_random_add = 2,
                cleanup_thread_preferred_points_per_iteration=0,
                skip_broken_part = true
        """
        )

        node.query("system stop merges tbl2")
        for i in range(50):
            # around 1k per block
            node.query("insert into tbl2 select number, 'A' from numbers(50)")

        node.query(
            """
            CREATE TABLE tbl3 (p UInt8, d String)
            ENGINE = MergeTree()
            ORDER BY p
            SETTINGS
                storage_policy = 'jbod',
                old_parts_lifetime = 1,
                cleanup_delay_period = 1,
                cleanup_delay_period_random_add = 2,
                cleanup_thread_preferred_points_per_iteration=0,
                skip_broken_part = true
        """
        )

        for i in range(50):
            # around 1k per block
            node.query("insert into tbl3 select number, 'A' from numbers(50)")

        # Mimic disk failure
        #
        # NOTE: you cannot do one of the following:
        # - chmod 000 - this will not block access to the owner of the namespace,
        #   and running clickhouse from non-root user is very tricky in this
        #   sandbox.
        # - unmount it, to replace with something else because in this case you
        #   will loose tmpfs and besides clickhouse works from root, so it will
        #   still be able to write/read from/to it.
        #
        # So it simply mounts over tmpfs, proc, and this will throw exception
        # for read, because there is no such file and does not allows writes
        # either.
        node.exec_in_container(
            ["bash", "-c", "mount -t proc proc /jbod1"], privileged=True, user="root"
        )

        time.sleep(20)

        assert (
            int(node.query("select total_space from system.disks where name = 'jbod1'"))
            == 0
        )

        # Test select
        assert int(node.query("select count(*) from tbl where d='A'")) > 0

        # Test insert
        for i in range(50):
            # around 1k per block
            node.query("insert into tbl select number, 'B' from numbers(50)")

        assert int(node.query("select count(*) from tbl where d='B'")) == 2500

        # Test drop partition
        node.query("alter table tbl3 drop partition tuple()")

        assert int(node.query("select count(*) from tbl3")) == 0

        # Test merge
        node.query("system start merges tbl2")

        for i in range(50):
            node.query("insert into tbl2 select number, 'B' from numbers(50)")

        node.query("optimize table tbl final")
        assert int(node.query("select count(*) from tbl2 where d='B'")) == 2500

        # Test create table
        node.query(
            """
            CREATE TABLE tbl4 (p UInt8, d String)
            ENGINE = MergeTree()
            ORDER BY p
            SETTINGS
                storage_policy = 'jbod',
                skip_broken_part = true
        """
        )

        for i in range(50):
            node.query("insert into tbl4 select number, 'B' from numbers(50)")

        assert int(node.query("select count(*) from tbl4 where d='B'")) == 2500

        # Mimic disk recovery
        #
        # NOTE: this will unmount only proc from /jbod1 and leave tmpfs
        node.exec_in_container(
            ["bash", "-c", "umount  /jbod1"], privileged=True, user="root"
        )

        node.restart_clickhouse()
        time.sleep(20)

        assert (
            int(node.query("select total_space from system.disks where name = 'jbod1'"))
            > 0
        )

    finally:
        node.query("DROP TABLE IF EXISTS tbl SYNC")
        node.query("DROP TABLE IF EXISTS tbl2 SYNC")
        node.query("DROP TABLE IF EXISTS tbl3 SYNC")
        node.query("DROP TABLE IF EXISTS tbl4 SYNC")
