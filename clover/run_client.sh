#!/bin/bash

Help=$(
    cat <<-"HELP"

run_client.sh - Run a Clover Client/Compute Node

Usage:
    ./run_client.sh <id>

Options:
    <id>   Unique Clover Node ID.
    -h     Show this message.

Examples:
    ./run_client.sh 1

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

LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes \
    numactl --cpunodebind=0 --membind=0 \
    ./init -C 1 -L 2 \
    --machine-id="$1" \
    --base-port-index=$ibdev_base_port --device-id=$ibdev_id \
    --num-clients=$NR_CN \
    --num-memory=$NR_MN \
    --memcached-server-ip="$MEMCACHED_SERVER_IP"
