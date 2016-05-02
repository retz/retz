#!/bin/bash

## TODO: "sleep" exections here and there should be replaces by
## appropriate check-and-wait loop for more robustness, or
## rewrite this script by Java/Scala/<your-fav-lang-here>.

trap 'echo "[ERROR] $0:$LINENO \"$BASH_COMMAND\"" ; exit 1' ERR
set -x

CURRENT=$(cd $(dirname $0) && pwd)
RES_DIR=${CURRENT}/../resources
BUILD_DIR=${CURRENT}/../../../build
LOG_DIR=${BUILD_DIR}/log
mkdir -p ${LOG_DIR}

docker build -t mesos-retz ./src/test/resources/docker

RETZ_CONTAINER=retz-inttest-ertpqgh34jv9air

(docker stop -t 1 ${RETZ_CONTAINER}; true)
sleep 3
docker run --rm --name=${RETZ_CONTAINER} \
       -p 15050:15050 -p 15051:15051 -p 19090:19090 \
       --privileged \
       -v ${BUILD_DIR}:/build mesos-retz &
sleep 5

# The tty mode (-t) of "docker exec" can not be used in gradle execution,
# so make it background without tty
docker exec -i $RETZ_CONTAINER /spawn_mesos_slave.sh > ${LOG_DIR}/mesos-agent.log 2>&1 &
docker exec -i $RETZ_CONTAINER /spawn_retz_server.sh > ${LOG_DIR}/retz-server.log 2>&1 &
sleep 8
docker exec -i $RETZ_CONTAINER ps awxx

${RES_DIR}/retz-client load-app -F file:///spawn_retz_server.sh -A echo
${RES_DIR}/retz-client list-app
${RES_DIR}/retz-client run -A echo -cmd 'echo foo' -R ${BUILD_DIR}/simple-test-out
${RES_DIR}/retz-client unload-app -A echo

docker stop -t 1 ${RETZ_CONTAINER}
