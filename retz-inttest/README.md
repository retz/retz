# Integration like test for Retz

## Gradle task

In `retz-inttest` project, there is a gradle task `inttest` to execute
simplest test. If everything goes well, all one should do is:

```
$ ../gradlew inttest
```

If it does not work, please read following chunk of duct-tape code and
workarounds.

## Docker Setup: CentOS 7

```
$ sudo yum install docker-engine
$ sudo systemctl enable docker.service
$ sudo systemctl start docker.service
```

For non-root users to use `docker` command without `sudo`, add the user
to docker group as:

```
$ sudo usermod -aG docker <username>
```

## Building docker image and run it

Following sample steps is assumed the working directory is `retz-inttest`. It's not
mandatory, just replacing paths should work (finger-crossed).

First, build docker image:

```
$ docker build -t mesos-retz ./src/test/resources/docker
```

Some preliminary work is done by gradle script:

```
$ ../gradlew copy
```

Then, run a container from the image above;

```
$ RETZ_CONTAINER=`docker run -d -p 5050:5050 -p 5051:5051 -p 9090:9090 --privileged \
     -v $(pwd)/build:/build mesos-retz`
```

Spawn mesos slave and retz server and confirm they are alive.

```
$ docker exec -it $RETZ_CONTAINER /spawn_mesos_slave.sh
$ docker exec -it $RETZ_CONTAINER /spawn_retz_server.sh
$ docker exec -it $RETZ_CONTAINER ps awxx
```

Now it's possible to kick retz client at host OS.

```
% ./src/test/resources/retz-client load-app -F file:///spawn_retz_server.sh -A echo
% ./src/test/resources/retz-client list-app
$ ./src/test/resources/retz-client run -A echo -cmd 'echo foo' -R ./out
$ ./src/test/resources/retz-client unload-app -A echo
```

To stop the container, it's better to add short timeout as `-t 1`:

```
$ docker stop -t 1 $RETZ_CONTAINER
```

## Docker related messy things

- To enable cgroups, which is needed by mesos agents, systemd is installed
  and some special steps are necessary. For details, refer
  http://developers.redhat.com/blog/2014/05/05/running-systemd-within-docker-container/ .

- Now port mapping is assumed to be static for mesos master, agent and retz-server.
  When we want to run multiple mesos instances, e.g. for parallel test
  execution, there should be some trick to pass mesos master IP:port to
  external world and use it from test handle.

## Misc one liners that may be useful

Attach with bash to detached docker container,

```
$ docker exec -it $RETZ_CONTAINER /bin/bash
```

then, it's possible to detach just `exit` from bash process.

Some examples to remove finished containers / images:

```
## Remove named container in the above run command
$ docker ps -a | grep '[r]etz-inttest' | awk '{print $1;}' | xargs docker rm

## Remove all exited containers
$ docker ps -a | grep Exited | awk '{print $1;}' | xargs docker rm
```

