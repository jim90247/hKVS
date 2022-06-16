#!/bin/bash
# A function to echo in blue color
function blue() {
	es=$(tput setaf 4)
	ee=$(tput sgr0)
	echo "${es}$1${ee}"
}

export HRD_REGISTRY_IP=${HRD_REGISTRY_IP:-"192.168.223.1"}
blue "HRD_REGISTRY_IP=${HRD_REGISTRY_IP}"

export MLX5_SINGLE_THREADED=1
export MLX4_SINGLE_THREADED=1

server_threads=32
worker_log=${1:-"/tmp/worker.log"}
blue "Saving a copy of worker log to ${worker_log}"

blue "Removing SHM key 24 (request region hugepages)"
sudo ipcrm -M 24

blue "Removing SHM keys used by MICA"
for i in $(seq 0 "$server_threads"); do
	key=$((3185 + i))
	sudo ipcrm -M $key 2>/dev/null
	key=$((4185 + i))
	sudo ipcrm -M $key 2>/dev/null
done

blue "Reset server QP registry"
pkill -2 memcached
memcached -I 1024m -m 2048 -l "$HRD_REGISTRY_IP" 1>/dev/null 2>/dev/null &
sleep 1

blue "Starting master process"
numactl --cpunodebind=0 --membind=0 ./main \
	--master 1 \
	--base-port-index 0 \
	--num-server-ports 1 &

# Give the master process time to create and register per-port request regions
sleep 1

blue "Starting worker threads"
# `stdbuf --output=L` makes stdout line-buffered even when redirected to file using tee
stdbuf --output=L \
	numactl --membind=0 ./main \
	--is-client 0 \
	--base-port-index 0 \
	--num-server-ports 1 \
	--postlist 32 | tee "$worker_log" &
