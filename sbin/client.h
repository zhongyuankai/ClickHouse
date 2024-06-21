#!/bin/bash
set -x

cd $(dirname $0)/..
tcp_port=`cat conf/config.xml |grep tcp_port|awk -F '>' '{print $2}'|awk -F '<' '{print $1}'`
password=`cat conf/users.xml |grep '<users>' -A 10|grep '<default>' -A 8|grep password|awk -F '>' '{print $2}'|awk -F '<' '{print $1}'`
bin/clickhouse-client --port=$tcp_port --password=$password -m

