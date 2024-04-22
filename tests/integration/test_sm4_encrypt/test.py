#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/kms.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_sm4_encrypt(started_cluster):
    ak = "c2131e634075b7eb019e671b543f6ada"
    sk = "08d779977f211bdb72b63ba13b830197"
    secret_id = "534543524554-677a3031-b087d64265e8000"
    version = "7085ff6305c9ab09"
    original_text = "1234567812345678"
    cipher_text = "ur8FCwh9ZCZegAAAAbh2bD5K+wQtOuST/crWVl55FdxGscyP9GtB1ESi8+uL"

    res = node1.query(
        f"""SELECT sm4Encrypt('{original_text}', '{ak}', '{sk}', '{secret_id}', '{version}');"""
    ).strip()
    assert cipher_text == res

    query_log = node1.query(
        "SELECT query FROM system.query_log WHERE query LIKE '%sm4Encrypt%' limit 1 ;"
    ).strip()
    assert (
        query_log
        == "SELECT sm4Encrypt(\\'1234567812345678\\', \\'xxx\\', \\'xxx\\', \\'xxx\\', \\'xxx\\');"
    )

    res = node1.query(f"""SELECT sm4Decrypt('{res}', '{ak}', '{sk}');""").strip()
    assert original_text == res

    query_log = node1.query(
        "SELECT query FROM system.query_log WHERE query LIKE '%sm4Decrypt%' limit 1;"
    ).strip()
    assert (
        query_log
        == "SELECT sm4Decrypt(\\'ur8FCwh9ZCZegAAAAbh2bD5K+wQtOuST/crWVl55FdxGscyP9GtB1ESi8+uL\\', \\'xxx\\', \\'xxx\\');"
    )

    node1.query("DROP TABLE IF EXISTS decrypt_tb;")
    node1.query(
        "CREATE TABLE decrypt_tb(original String, cipher String) ENGINE = MergeTree() ORDER BY original;"
    )
    node1.query(
        f"""INSERT INTO decrypt_tb SELECT concat('1234567812345678_xxxxxxxx_', toString(number)) original, sm4Encrypt(original, '{ak}', '{sk}', '{secret_id}', '{version}') cipher FROM numbers(500000);"""
    )

    count = int(
        node1.query(
            f"""select countIf(original = sm4Decrypt(cipher, '{ak}', '{sk}')) from decrypt_tb;"""
        )
    )
    assert count == 500000
