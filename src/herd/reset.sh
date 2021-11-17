#!/bin/bash
set -xe

bash update-param.sh
make clean
make
