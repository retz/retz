#!/bin/bash
#
#    Retz
#    Copyright (C) 2016 Nautilus Technologies, Inc.
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

RETZ_CONTAINER=$1
CURRENT=$(cd $(dirname $0) && pwd)

docker exec -it $RETZ_CONTAINER /spawn_mesos_slave.sh
docker exec -it $RETZ_CONTAINER /spawn_retz_server.sh

docker exec -it $RETZ_CONTAINER ps awxx | egrep -E '(mesos-slave|mesos-master|retz)'

${CURRENT}/retz-client load-app -F file:///spawn_retz_server.sh -A echo

${CURRENT}/retz-client list-app

${CURRENT}/retz-client run -A echo -cmd 'echo HOGEEEEEEEEEEEEEEEEEE' -R ./out

# docker stop -t 1 $RETZ_CONTAINER
