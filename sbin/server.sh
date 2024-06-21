#!/bin/bash
set -x

cd $(dirname $0)/..
base_dir=`pwd`
command=$1

usage="Usage: server.sh start|stop"

case $command in
  (start)
    #export LD_LIBRARY_PATH="$base_dir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    nohup $base_dir/bin/clickhouse-server --config-file=$base_dir/conf/config.xml 2>&1 &
    ;;
  (stop)
    if pkill -f "$base_dir/bin/clickhouse-server --config-file=$base_dir/conf/config.xml";then
      echo 'STOPPED'
    else
      echo 'FAILED'
    fi
    ;;
  (*)
    echo $usage
    exit 1
    ;;
esac

