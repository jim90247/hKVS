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
memcached -l "$HRD_REGISTRY_IP" 1>/dev/null 2>/dev/null &
sleep 1

blue "Starting master process"
numactl --cpunodebind=0 --membind=0 ./echo_server \
	--master 1 \
	--base-port-index 0 \
	--num-server-ports 1 &

# Give the master process time to create and register per-port request regions
sleep 1

blue "Starting worker threads"
# `stdbuf --output=L` makes stdout line-buffered even when redirected to file using tee
stdbuf --output=L \
	numactl --membind=0 ./echo_server \
	--base-port-index 0 \
	--num-server-ports 1 \
	--postlist 32
