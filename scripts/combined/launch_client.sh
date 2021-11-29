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

num_threads=16 # Threads per client machine
show_message "Running $num_threads client threads"

sudo LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-"$HOME/.local/lib"}" -E \
	numactl --cpunodebind=0 --membind=0 "${bindir}/combined_client" \
	--herd_base_port_index 0 \
	--threads $num_threads \
	--update_percentage 5 \
	--herd_machine_id "$1"
