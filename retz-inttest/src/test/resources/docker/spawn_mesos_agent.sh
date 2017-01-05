#!/bin/bash
#
#    Retz
#    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#


set -e
set -x

IP_ADDR=`ip -f inet address show eth0 | grep inet | awk {'print $2;}' | cut -d/ -f1`
echo "eth0 IP address for mesos-master: " ${IP_ADDR}

mkdir -p /sys/fs/cgroup/systemd/mesos_executors.slice

echo "Directory created."

nohup /sbin/mesos-slave --master=${IP_ADDR}:15050 \
      --port=15051 \
      --work_dir=/var/lib/mesos > /build/log/mesos-agent.log 2>&1 &
