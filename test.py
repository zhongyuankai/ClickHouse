#!/usr/bin/env python3
import random
import signal
import string
import subprocess
import sys
import os
import tempfile

from argparse import ArgumentParser
from datetime import datetime

docker_images_origin = [
    "clickhouse-test/test-util:v701",
    "clickhouse-test/binary-builder:v701",
    "clickhouse-test/test-base:v701",
    "clickhouse-test/style-test:v701",
    "clickhouse-test/unit-test:v701",
    "clickhouse-test/fasttest:v701",
    "clickhouse-test/stateless-test:v701",
    "clickhouse-test/sqlancer-test:v701",
    "clickhouse-test/sqllogic-test:v701",
    "clickhouse-test/sqltest:v701",
    "clickhouse-test/stateful-test:v701",
    "clickhouse-test/integration-test:v701",
    "clickhouse-test/integration-helper:v701",
    "clickhouse-test/integration-tests-runner:v701",
    "clickhouse-test/fuzzer:v701",
]

DOCKER_IMAGES = docker_images_origin

DOCKER_PREFIX = "hub.xiaojukeji.com/chrischenwei/"

TEST_LIST = [
    "style",
    "unit",
    "fasttest",
    "stateless",
    "sqlancer",
    "sqllogic",
    "sqltest",
    "stateful",
    "stress",
    "integration",
]

CONTAINER_NAME = "container_name"


def get_buider_cmd(
    clickhouse_src_path: str,
    clickhouse_build_output_path: str,
    package_type: str,
    ccache_path: str,
    is_debug=False,
    sanitizer="",
) -> str:
    debug = ""
    if is_debug:
        debug = "--debug-build"

    ccache_option = ""
    if ccache_path:
        ccache_option = f"--cache ccache --ccache-dir {ccache_path}"

    cmd = (
        f"{clickhouse_src_path}/docker/packager/packager --clickhouse-repo-path {clickhouse_src_path} --output-dir "
        f"{clickhouse_build_output_path} --package-type {package_type} --compiler=clang-16 {ccache_option} "
        f"{debug} {sanitizer}"
    )

    return cmd


def get_style_cmd(clickhouse_src_path: str, test_output_root: str) -> str:
    cmd = (
        f"docker run -u 1000:1000 --rm --name {CONTAINER_NAME} --cap-add=SYS_PTRACE "
        f"--volume={clickhouse_src_path}:/ClickHouse "
        f"--volume={test_output_root}/test_output_style:/test_output clickhouse-test/style-test:v701"
    )
    return cmd


def get_unit_cmd(unit_tests_dbms_path: str, test_output_root: str) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --cap-add=SYS_PTRACE --volume={unit_tests_dbms_path}"
        f":/unit_tests_dbms --volume={test_output_root}/test_output_unit:/test_output clickhouse-test/unit-test:v701"
    )
    return cmd


def get_fasttest_cmd(clickhouse_src_path: str, test_output_root: str) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --cap-add=SYS_PTRACE --network=host "
        "-e FASTTEST_WORKSPACE=/fasttest-workspace -e "
        "FASTTEST_OUTPUT=/test_output -e FASTTEST_SOURCE=/ClickHouse --cap-add=SYS_PTRACE -e "
        "FASTTEST_CMAKE_FLAGS='-DCOMPILER_CACHE=sccache' -e COPY_CLICKHOUSE_BINARY_TO_OUTPUT=1 "
        f"--volume={test_output_root}/test_output_fasttest:/fasttest-workspace "
        f"--volume={clickhouse_src_path}:/ClickHouse "
        f"--volume={test_output_root}/test_output_fasttest:/test_output clickhouse-test/fasttest:v701"
    )
    return cmd


def get_stateless_cmd(
    clickhouse_deb_package_path: str,
    clickhouse_src_path: str,
    test_output_root: str,
    test_list: str,
) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --volume={clickhouse_deb_package_path}:/package_folder "
        f"--volume={clickhouse_src_path}/tests:/usr/share/clickhouse-test "
        f"--volume={clickhouse_src_path}/tests/analyzer_tech_debt.txt:/analyzer_tech_debt.txt "
        f"--volume={test_output_root}/test_output_stateless:/test_output "
        f"--volume={test_output_root}/test_output_stateless_server_log/:/var/log/clickhouse-server "
        "--cap-add=SYS_PTRACE -e "
        'S3_URL="https://s3.amazonaws.com/clickhouse-datasets" -e ADDITIONAL_OPTIONS="--hung-check --print-time '
        f'--order asc --no-random-settings --no-random-merge-tree-settings {test_list} " '
        "clickhouse-test/stateless-test:v701"
    )
    return cmd


def get_sqlancer_cmd(clickhouse_deb_package_path: str, test_output_root: str) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --privileged --network=host "
        f"--volume={test_output_root}/test_output_sqlancer:/workspace "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
        f"--volume={clickhouse_deb_package_path}:/programs clickhouse-test/sqlancer-test:v701"
    )
    return cmd


def get_sqllogic_cmd(
    clickhouse_src_path: str, clickhouse_deb_package_path: str, test_output_root: str
) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --volume={clickhouse_deb_package_path}:/package_folder "
        f"--volume={clickhouse_src_path}/tests:/clickhouse-tests "
        f"--volume={test_output_root}/test_output_sqllogic:/test_output "
        f"--volume={test_output_root}/test_output_sqllogic_server_log:/var/log/clickhouse-server "
        "--cap-add=SYS_PTRACE clickhouse-test/sqllogic-test:v701"
    )
    return cmd


def get_sqltest_cmd(
    clickhouse_bin_package_debug_path: str, test_output_root: str
) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --privileged --network=host "
        f"--volume={test_output_root}/test_output_sqltest:/workspace "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
        f"--volume={clickhouse_bin_package_debug_path}:/binary clickhouse-test/sqltest:v701"
    )
    return cmd


def get_stateful_cmd(
    clickhouse_src_path: str,
    clickhouse_deb_package_path: str,
    clickhouse_pre_dataset: str,
    test_output_root: str,
    test_list: str,
) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --volume={clickhouse_deb_package_path}:/package_folder "
        f"--volume={clickhouse_src_path}/tests:/usr/share/clickhouse-test "
        f"--volume={clickhouse_src_path}/tests/analyzer_tech_debt.txt:/analyzer_tech_debt.txt "
        f"--volume={test_output_root}/test_output_stateful:/test_output "
        f"--volume={test_output_root}/test_output_stateful_server_log:/var/log/clickhouse-server "
        f"--volume={clickhouse_pre_dataset}:/dataset --cap-add=SYS_PTRACE "
        '-e S3_URL="https://s3.amazonaws.com/clickhouse-datasets" -e ADDITIONAL_OPTIONS="--hung-check '
        f'--print-time --order asc --no-random-settings --no-random-merge-tree-settings {test_list} " '
        "clickhouse-test/stateful-test:v701"
    )
    return cmd


def get_integration_cmd(
    clickhouse_src_path: str,
    clickhouse_deb_package_path: str,
    test_output_root: str,
    test_list=None,
) -> str:
    tests_str = ""
    if test_list:
        tests_str = "-t " + test_list
    cmd = (
        f"{clickhouse_src_path}/tests/integration/runner --binary {clickhouse_deb_package_path}/clickhouse "
        f"--odbc-bridge-binary {clickhouse_deb_package_path}/clickhouse-odbc-bridge --clickhouse-root "
        f"{clickhouse_src_path}/ --cases-dir /{test_output_root}/test_output_integration/integration/ -n 8 {tests_str}"
    )
    return cmd


def get_stress_cmd(
    clickhouse_src_path: str,
    clickhouse_deb_package_path: str,
    clickhouse_pre_dataset: str,
    test_output_root: str,
) -> str:
    cmd = (
        f"docker run --rm --name {CONTAINER_NAME} --cap-add=SYS_PTRACE --privileged "
        "-e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' "
        f"--volume={clickhouse_deb_package_path}:/package_folder "
        f"--volume={test_output_root}/test_output_stress:/test_output "
        f"--volume={clickhouse_src_path}/tests:/usr/share/clickhouse-test "
        f"--volume={test_output_root}/test_output_stress_server_log:/var/log/clickhouse-server "
        f"--volume={clickhouse_pre_dataset}:/dataset "
        "clickhouse-test/stress-test:v701"
    )
    return cmd


def get_fuzzer_cmd(clickhouse_src_path: str, clickhouse_deb_package_path: str) -> str:
    print(clickhouse_src_path, clickhouse_deb_package_path)
    cmd = ""
    return cmd


"""
5个参数：
clickhouse_src_path 源代码目录
clickhouse_deb_package_path deb 包所在的路径，是binary-builder 编译输出目录
clickhouse_pre_dataset 预置的表数据
test_output_root 测试输出根路径
unit_tests_dbms_path  unit_test_dbms 可执行文件的路径，binary-builder 编译一个bin release 版本输出获得
clickhouse_bin_package_debug_path debug bin 包所在的路径
"""


def parse_args():
    parser = ArgumentParser(description="ClickHouse test tool")

    parser.add_argument(
        "--clickhouse-src",
        default=None,
        dest="clickhouse_src_path",
        help="Path to repository root folder. default is `pwd`.",
    )
    parser.add_argument(
        "--test_output_path",
        default=None,
        dest="test_output_root_path",
        help="clickhouse test output root path, each test create subdirectory inside",
    )
    parser.add_argument(
        "--ck_deb_package_path",
        default=None,
        dest="ck_deb_path",
        help="clickhouse release deb package path, if not set, will build and save to "
        "test_output_path/test_output_deb_package.",
    )
    parser.add_argument(
        "--ck_bin_path",
        default=None,
        dest="ck_bin_path",
        help="clickhouse debug bin package path, if not set, will build and save to "
        "test_output_path/test_output_bin_package.",
    )
    parser.add_argument(
        "--ck_bin_path_debug",
        default=None,
        dest="ck_bin_path_debug",
        help="clickhouse debug bin package path, if not set, will build and save to "
        "test_output_path/test_output_bin_package_debug.",
    )
    parser.add_argument(
        "--ck_pre_dataset_path",
        default=None,
        dest="ck_pre_dataset",
        help="clickhouse pre dataset path, stateful and stress test are dependent.",
    )
    parser.add_argument(
        "-t",
        "--test_list",
        action="store",
        nargs="+",
        default=[],
        dest="test_list",
        help="List of tests to run. all tests: [fasttest, style, unit, stateless, sqlancer, sqllogic, sqltest, "
        "stateful, stress, integration]",
    )
    parser.add_argument(
        "--exclude_test_list",
        action="store",
        nargs="+",
        default=[],
        dest="exclude_test_list",
        help="List of tests excluded to run",
    )
    parser.add_argument(
        "--ccache_path",
        default=None,
        dest="ccache_path",
        help="ccache root path",
    )
    parser.add_argument(
        "--stateless_test_list",
        action="store",
        nargs="+",
        default=[],
        dest="stateless_test_list",
        help="special stateless tests to run, default run all tests",
    )
    parser.add_argument(
        "--stateful_test_list",
        action="store",
        nargs="+",
        default=[],
        dest="stateful_test_list",
        help="special stateful tests to run, default run all tests",
    )
    parser.add_argument(
        "--integration_test_list",
        action="store",
        nargs="+",
        default=[],
        dest="integration_test_list",
        help="special integration tests to run, default run all tests",
    )

    return parser.parse_args()


package_dependent = {
    "": [],
}

TEST_DEPENDENT = {
    "style": None,
    "unit": "bin",
    "fasttest": None,
    "stateless": "deb",
    "sqlancer": "deb",
    "sqllogic": "deb",
    "sqltest": "debug",
    "stateful": "deb",
    "integration": "deb",
    "stress": "deb",
}


def print_info(name: str):
    print(
        "\n\n**********************************************************************************************\n"
        "***************     "
        + name
        + " ["
        + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        + "]  \n"
        "**********************************************************************************************\n"
    )


def random_str(length=6):
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.SystemRandom().choice(alphabet) for _ in range(length))


def docker_kill_handler_handler(signum, frame):
    subprocess.check_call(
        "docker stop {name}".format(name=CONTAINER_NAME),
        shell=True,
    )
    raise KeyboardInterrupt("Killed by Ctrl+C")


signal.signal(signal.SIGINT, docker_kill_handler_handler)


def main(args):
    print(args)
    current_path = os.getcwd()

    clickhouse_src_path = current_path  # 源代码目录
    test_output_root = ""  # 测试输出根路径
    clickhouse_deb_package_path = None  # deb 包所在的路径，是binary-builder 编译输出目录，如果未指定则编译二进制
    clickhouse_pre_dataset = None  # 预置的表数据
    unit_tests_dbms_path = None  # unit_test_dbms 可执行文件的路径，编译bin包会生成，如果未指定则编译二进制
    clickhouse_bin_package_debug_path = (
        None  # debug bin 包所在的路径，如果未指定则编译二进制，只有在有测试需要的时候才编译
    )
    ccache_path = None

    if args.test_output_root_path:
        test_output_root = args.test_output_root_path
    else:
        test_output_root = os.path.join(
            current_path + "/test_output_" + datetime.now().strftime("%Y%m%d%H%M")
        )
        if not os.path.exists(test_output_root):
            os.makedirs(test_output_root)

    if args.clickhouse_src_path:
        print(args.clickhouse_src_path)
        clickhouse_src_path = args.clickhouse_src_path

    if args.ccache_path:
        ccache_path = args.ccache_path

    # copy clickhouse-repo
    copy_repo = os.path.join(test_output_root, "didi-clickhouse")
    if os.path.exists(copy_repo):
        subprocess.check_call(f"rm -rf {copy_repo}", shell=True)

    subprocess.check_call(f"cp -r {clickhouse_src_path} {test_output_root}", shell=True)
    clickhouse_src_path = copy_repo

    test_list = []
    exclude_test_list = []

    stateless_test_list = []
    stateful_test_list = []
    integration_test_list = []

    # get test list
    print("args.test_list:", args.test_list)
    if not args.test_list:
        test_list = TEST_LIST
    else:
        test_list = args.test_list

    if args.exclude_test_list:
        exclude_test_list = args.exclude_test_list

    real_test_list = []
    need_deb_package = False
    need_bin_package = False
    need_bin_package_debug = False

    for test in test_list:
        if test not in exclude_test_list:
            real_test_list.append(test)
            if TEST_DEPENDENT[test] == "deb":
                need_deb_package = True
            elif TEST_DEPENDENT[test] == "bin":
                need_bin_package = True
            elif TEST_DEPENDENT[test] == "debug":
                need_bin_package_debug = True

    need_build_deb_package = False
    need_build_bin_package = False
    need_build_bin_package_debug = False

    if need_deb_package:
        if args.ck_deb_path:
            clickhouse_deb_package_path = args.ck_deb_path
        else:
            clickhouse_deb_package_path = os.path.join(
                test_output_root, "test_output_deb_package"
            )
            need_build_deb_package = True

    if need_bin_package:
        if args.ck_bin_path:
            unit_tests_dbms_path = args.ck_bin_path
        else:
            unit_tests_dbms_path = os.path.join(
                test_output_root, "test_output_bin_package"
            )
            need_build_bin_package = True

    if need_bin_package_debug:
        if args.ck_bin_path_debug:
            clickhouse_bin_package_debug_path = args.ck_bin_path
        else:
            clickhouse_bin_package_debug_path = os.path.join(
                test_output_root, "test_output_bin_package_debug"
            )
            need_build_bin_package_debug = True

    if args.ck_pre_dataset:
        clickhouse_pre_dataset = args.ck_pre_dataset
    else:
        if "stress" in test_list or "stateful" in test_list:
            print(
                "error, stress tests and stateful test need pre dataset, please set --ck_pre_dataset_path"
            )
            exit(1)

    if args.stateless_test_list:
        stateless_test_list = args.stateless_test_list

    if args.stateful_test_list:
        stateful_test_list = args.stateful_test_list

    if args.integration_test_list:
        integration_test_list = args.integration_test_list

    print(
        "======== test params ========"
        "\n ck_src_path: "
        + (clickhouse_src_path if clickhouse_src_path else "")
        + "\n test_output_path: "
        + test_output_root
        + "\n ck_pre_dataset_path: "
        + (clickhouse_pre_dataset if clickhouse_pre_dataset else "")
        + "\n ck_deb_package_path: "
        + (clickhouse_deb_package_path if clickhouse_deb_package_path else "")
        + "\t (need_build:"
        + ("1" if need_build_deb_package else "0")
        + ")"
        "\n ck_bin_path: "
        + (unit_tests_dbms_path if unit_tests_dbms_path else "")
        + "\t (need_build:"
        + ("1" if need_build_bin_package else "0")
        + ")"
        "\n ck_bin_path_debug: "
        + (
            clickhouse_bin_package_debug_path
            if clickhouse_bin_package_debug_path
            else ""
        )
        + "\t (need_build:"
        + ("1" if need_build_bin_package_debug else "0")
        + ")"
        "\n test_list: " + str(real_test_list) + "\n\n"
    )

    # 0 style check
    # style
    if "style" in real_test_list:
        print_info("style test start")
        style_test_out = os.path.join(test_output_root, "test_output_style")
        if not os.path.exists(style_test_out):
            os.makedirs(style_test_out)

        global CONTAINER_NAME
        CONTAINER_NAME = f"style_{random_str()}"

        cmd = get_style_cmd(clickhouse_src_path, test_output_root)

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("style test error")

    # 1 run fasttest
    print_info("tests start")

    if "fasttest" in real_test_list:
        print_info("fasttest start")
        fasttest_test_out = os.path.join(test_output_root, "test_output_fasttest")
        if not os.path.exists(fasttest_test_out):
            os.makedirs(fasttest_test_out)

        CONTAINER_NAME = f"fasttest_{random_str()}"

        cmd = get_fasttest_cmd(clickhouse_src_path, test_output_root)

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("fasttest error")

    # 2 build binary needs
    if need_build_deb_package:
        print_info("deb package build start")
        if not os.path.exists(clickhouse_deb_package_path):
            os.makedirs(clickhouse_deb_package_path)

        CONTAINER_NAME = f"build_{random_str()}"

        ccache_path_deb = None
        if ccache_path:
            ccache_path_deb = os.path.join(ccache_path, "ccache_deb")
            if not os.path.exists(ccache_path_deb):
                os.makedirs(ccache_path_deb)

        deb_cmd = get_buider_cmd(
            clickhouse_src_path, clickhouse_deb_package_path, "deb", ccache_path_deb
        )
        try:
            subprocess.check_call(deb_cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("build deb package error")

    if need_build_bin_package:
        print_info("bin release build start")
        if not os.path.exists(unit_tests_dbms_path):
            os.makedirs(unit_tests_dbms_path)

        CONTAINER_NAME = f"build_{random_str()}"

        ccache_path_bin = None
        if ccache_path:
            ccache_path_bin = os.path.join(ccache_path, "ccache_bin")
            if not os.path.exists(ccache_path_bin):
                os.makedirs(ccache_path_bin)

        bin_cmd = get_buider_cmd(
            clickhouse_src_path, unit_tests_dbms_path, "binary", ccache_path_bin
        )
        try:
            subprocess.check_call(bin_cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("build bin package error")

    if need_build_bin_package_debug:
        print_info("bin debug build start")
        if not os.path.exists(clickhouse_bin_package_debug_path):
            os.makedirs(clickhouse_bin_package_debug_path)

        CONTAINER_NAME = f"build_{random_str()}"

        ccache_path_bin_debug = None
        if ccache_path:
            ccache_path_bin_debug = os.path.join(ccache_path, "ccache_bin_debug")
            if not os.path.exists(ccache_path_bin_debug):
                os.makedirs(ccache_path_bin_debug)

        debug_cmd = get_buider_cmd(
            clickhouse_src_path,
            clickhouse_bin_package_debug_path,
            "binary",
            ccache_path_bin_debug,
            True,
        )
        try:
            subprocess.check_call(debug_cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("build deb package error")

    # 3 run test
    # unit
    if "unit" in real_test_list:
        print_info("unit test start")
        unit_test_out = os.path.join(test_output_root, "test_output_unit")
        if not os.path.exists(unit_test_out):
            os.makedirs(unit_test_out)

        CONTAINER_NAME = f"unit_{random_str()}"

        cmd = get_unit_cmd(unit_tests_dbms_path, test_output_root)

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("unit test error")

    # stateless
    if "stateless" in real_test_list:
        print_info("stateless test start")
        stateless_test_out = os.path.join(test_output_root, "test_output_stateless")
        if not os.path.exists(stateless_test_out):
            os.makedirs(stateless_test_out)

        stateless_server_log = os.path.join(
            test_output_root, "test_output_stateless_server_log"
        )
        if not os.path.exists(stateless_server_log):
            os.makedirs(stateless_server_log)

        test_list_str = ""
        if stateless_test_list:
            test_list_str = " ".join(stateless_test_list)

        CONTAINER_NAME = f"stateless_{random_str()}"

        cmd = get_stateless_cmd(
            clickhouse_deb_package_path,
            clickhouse_src_path,
            test_output_root,
            test_list_str,
        )

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("stateless test error")

    # sqlancer
    if "sqlancer" in real_test_list:
        print_info("sqlancer test start")
        sqlancer_test_out = os.path.join(test_output_root, "test_output_sqlancer")
        if not os.path.exists(sqlancer_test_out):
            os.makedirs(sqlancer_test_out)

        CONTAINER_NAME = f"sqlancer_{random_str()}"

        cmd = get_sqlancer_cmd(clickhouse_deb_package_path, test_output_root)

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("sqlancer test error")

    # sqllogic
    if "sqllogic" in real_test_list:
        print_info("sqllogic test start")
        sqllogic_server_log = os.path.join(
            test_output_root, "test_output_sqllogic_sever_log"
        )
        if not os.path.exists(sqllogic_server_log):
            os.makedirs(sqllogic_server_log)

        sqllogic_test_out = os.path.join(test_output_root, "test_output_sqllogic")
        if not os.path.exists(sqllogic_test_out):
            os.makedirs(sqllogic_test_out)

        CONTAINER_NAME = f"sqllogic_{random_str()}"

        cmd = get_sqllogic_cmd(
            clickhouse_src_path, clickhouse_deb_package_path, test_output_root
        )

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("sqllogic test error")

    # sqltest
    if "sqltest" in real_test_list:
        print_info("sqltest start")
        sqltest_test_out = os.path.join(test_output_root, "test_output_sqltest")
        if not os.path.exists(sqltest_test_out):
            os.makedirs(sqltest_test_out)

        CONTAINER_NAME = f"sqltest_{random_str()}"

        cmd = get_sqltest_cmd(clickhouse_bin_package_debug_path, test_output_root)

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("sqltest error")

    # stateful
    if "stateful" in real_test_list:
        print_info("stateful test start")
        stateful_test_out = os.path.join(test_output_root, "test_output_stateful")
        if not os.path.exists(stateful_test_out):
            os.makedirs(stateful_test_out)

        stateful_server_log = os.path.join(
            test_output_root, "test_output_stateful_server_log"
        )
        if not os.path.exists(stateful_server_log):
            os.makedirs(stateful_server_log)

        test_list_str = ""
        if stateful_test_list:
            test_list_str = " ".join(stateful_test_list)

        CONTAINER_NAME = f"stateful_{random_str()}"

        cmd = get_stateful_cmd(
            clickhouse_src_path,
            clickhouse_deb_package_path,
            clickhouse_pre_dataset,
            test_output_root,
            test_list_str,
        )

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("stateful test error")

    # stress
    if "stress" in real_test_list:
        print_info("stress test start")
        stress_test_out = os.path.join(test_output_root, "test_output_stress")
        if not os.path.exists(stress_test_out):
            os.makedirs(stress_test_out)

        stress_server_log = os.path.join(
            test_output_root, "test_output_stress_server_log"
        )
        if not os.path.exists(stress_server_log):
            os.makedirs(stress_server_log)

        CONTAINER_NAME = f"stress_{random_str()}"

        cmd = get_stress_cmd(
            clickhouse_src_path,
            clickhouse_deb_package_path,
            clickhouse_pre_dataset,
            test_output_root,
        )

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("stress test error")

    # integration
    if "integration" in real_test_list:
        print_info("integration test start")
        integration_test_out = os.path.join(test_output_root, "test_output_integration")
        if not os.path.exists(integration_test_out):
            os.makedirs(integration_test_out)

        integration_tests = os.path.join(integration_test_out, "integration")
        if os.path.exists(integration_tests):
            subprocess.check_call(f"rm -rf {integration_tests}", shell=True)

        subprocess.check_call(
            f"cp -r {clickhouse_src_path}/tests/integration {integration_test_out}",
            shell=True,
        )

        test_list_str = None
        if integration_test_list:
            test_list_str = " ".join(integration_test_list)

        CONTAINER_NAME = f"integration_{random_str()}"

        cmd = get_integration_cmd(
            clickhouse_src_path,
            clickhouse_deb_package_path,
            test_output_root,
            test_list_str,
        )

        try:
            subprocess.check_call(cmd, shell=True)
        except Exception as ex:
            print("error", ex)
            raise Exception("integration test error")

    print_info("all test finished")


if __name__ == "__main__":
    try:
        args = parse_args()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    main(args)
