#!/bin/bash

function show_message() {
    echo "$(date +%Y-%m-%dT%H:%M:%S)|" "$@"
}

function abort_exec() {
    reason=$*
    show_message "ERROR:" "${reason}"
    exit 1
}

bindir="$(readlink -f "$(dirname "$0")/../../bin")"


source "$(dirname "$0")/export_local_settings.sh"

[ -f "${bindir}/herd" ] || abort_exec "Please install herd in ${bindir}"
[ -f "${bindir}/combined_worker" ] || abort_exec "Please install combined_worker in ${bindir}"

server_threads=8
worker_log=${1:-/tmp/worker.log}
show_message "Saving a copy of worker log to ${worker_log}"

show_message "Removing SHM key 24 (request region hugepages)"
sudo ipcrm -M 24

show_message "Removing SHM keys used by MICA"
for i in $(seq 0 "$server_threads"); do
    key=$((3185 + i))
    sudo ipcrm -M $key 2>/dev/null
    key=$((4185 + i))
    sudo ipcrm -M $key 2>/dev/null
done

show_message "Reset server QP registry"
sudo pkill memcached
memcached -u root -I 1024m -m 2048 -l "$HRD_REGISTRY_IP" 1>/dev/null 2>/dev/null &
sleep 1
show_message "Memcached server IP: $HRD_REGISTRY_IP"

show_message "Starting master process"
numactl --cpunodebind=0 --membind=0 "${bindir}/herd" \
    --master 1 \
    --base-port-index 0 \
    --num-server-ports 1 &

# Give the master process time to create and register per-port request regions
sleep 1

show_message "Starting workers"
# `stdbuf --output=L` makes stdout line-buffered even when redirected to file using tee
stdbuf --output=L \
    numactl --cpunodebind=0 --membind=0 "${bindir}/combined_worker" \
    --herd_base_port_index 0 \
    --postlist 8 \
    --clover_machine_id 1 \
    --clover_ib_dev $CLOVER_IB_DEV \
    --clover_ib_port 1 \
    --clover_cn 2 \
    --clover_dn 1 \
    --clover_memcached_ip "$HRD_REGISTRY_IP" 2>&1 | tee "$worker_log" &
