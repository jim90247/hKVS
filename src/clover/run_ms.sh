#!/bin/bash

Help=$(
    cat <<-"HELP"

run_ms.sh - Run a Clover Metadata Server

Usage:
    ./run_ms.sh <id>

Options:
    <id>   Unique Clover Node ID.
    -h     Show this message.

Examples:
    ./run_ms.sh 0

HELP
)

help() {
    echo "$Help"
}

if [[ $# == 0 ]] || [[ "$1" == "-h" ]]; then
    help
    exit 1
fi

source export_local_settings.sh
source export_experiment_settings.sh

MLX5_SINGLE_THREADED=1 \
    numactl --cpunodebind=0 --membind=0 \
    ./init -S 1 -L 2 \
    --machine-id="$1" \
    --base-port-index=$ibdev_base_port --device-id=$ibdev_id \
    --num-clients=$NR_CN \
    --num-memory=$NR_MN \
    --memcached-server-ip="$MEMCACHED_SERVER_IP"
