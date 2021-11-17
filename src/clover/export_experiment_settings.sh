#!/bin/bash

#
# Configuration
#
# NR_CN: number of computing/client nodes
# NR_MN: number of memory nodes
# MEMCACHED_SERVER_IP: ip of memcached server instance
#
export NR_CN=1 NR_MN=1 MEMCACHED_SERVER_IP=${MEMCACHED_SERVER:-"192.168.223.1"}
