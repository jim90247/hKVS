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

export HRD_REGISTRY_IP=${HRD_REGISTRY_IP:-"192.168.223.1"}
export MLX5_SINGLE_THREADED=1
export MLX4_SINGLE_THREADED=1

source "$(dirname "$0")/export_local_settings.sh"

show_message "Memcached server IP: ${HRD_REGISTRY_IP}"
if ! ping "$HRD_REGISTRY_IP" -c 1 >/dev/null; then
	abort_exec "Memcached server is not reachable"
fi

[ "$#" -eq 1 ] || abort_exec "Illegal number of parameters."

[ -f "${bindir}/combined_client" ] || abort_exec "Please install combined_client in ${bindir}"

"${bindir}/combined_client" \
	--herd_base_port_index 0 \
	--herd_threads 24 \
	--update_percentage 0 \
	--zipfian_alpha 0.99 \
	--herd_machine_id "$1" \
	--clover_machine_id $(($1 + 2)) \
	--clover_ib_dev $CLOVER_IB_DEV \
	--clover_ib_port 1 \
	--clover_cn 4 \
	--clover_dn 1 \
	--clover_threads 16 \
	--clover_coros 4 \
	--clover_memcached_ip "$HRD_REGISTRY_IP"
