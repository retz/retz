#!/bin/bash
#
#    Retz
#    Copyright (C) 2016 Nautilus Technologies, KK.
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

IP_ADDR=`ip -f inet address show eth0 | grep inet | awk {'print $2;}' | cut -d/ -f1`
echo "eth0 IP address used by retz-server: " ${IP_ADDR}

sed -i -e 's/##IP##/'${IP_ADDR}'/' retz.properties

nohup java -jar /build/libs/retz-server-all.jar \
    -C /retz.properties > ./retz-server.nohup.out &

sleep 1
