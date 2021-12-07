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

export HRD_REGISTRY_IP="192.168.223.1"
export MLX5_SINGLE_THREADED=1
export MLX4_SINGLE_THREADED=1

[ "$#" -eq 1 ] || abort_exec "Illegal number of parameters."

sudo LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-"$HOME/.local/lib"}" -E \
	numactl --cpunodebind=0 --membind=0 "${bindir}/combined_client" \
	--herd_base_port_index 0 \
	--herd_threads 8 \
	--update_percentage 5 \
	--zipfian_alpha 0.99 \
	--herd_machine_id "$1" \
	--clover_machine_id 2 \
	--clover_ib_dev 1 \
	--clover_ib_port 1 \
	--clover_cn 2 \
	--clover_dn 1 \
	--clover_threads 8 \
	--clover_memcached_ip "$HRD_REGISTRY_IP"
