#!/bin/bash

#
# Configuration
#
# ibdev_id: Specify your IB DEVICE ID starting from 0.
#           Follow the sequence of ibv_devinfo.
# ibdev_base_port: the port index of @ibdev_id.
# Therefore, @ibdev_id + @ibdev_base_port identify one specific IB dev port.
#
export ibdev_id=1 ibdev_base_port=1
