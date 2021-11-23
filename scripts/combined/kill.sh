#!/bin/bash
set -x

sudo pkill main
sudo pkill memcached
sudo pkill combined_worker
