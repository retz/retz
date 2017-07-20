# Integration like test for Retz

## Gradle task

In `retz-inttest` project, there is a gradle task `inttest` to execute
simplest test. If everything goes well, all one should do is:

```
$ ../gradlew test -Dinttest
```

If it does not work, please read following chunk of duct-tape code and
workarounds.

In MacOS, you may need Docker host listening to TCP rather than Unix
domain socket, in another console:

```
$ sudo socat TCP-LISTEN:2375,reuseaddr,fork UNIX-CONNECT:/var/run/docker.sock
```

Then set `DOCKER_HOST=tcp://127.0.0.1:2375` as an environment variable where
you run inttest.

If integration testing with old Mesos versions, define `mesos_version` as an
Gradle argument. Currently `1.2.1-2.0.1` and `1.3.0-2.0.3` are supported.
If it is not defined, just `mesos` will be installed by YUM. Example:

```
$ ../gradlew test -Dinttest -Dmesos_version=1.2.1-2.0.1
```

Supported Mesos version is defined in toplevel `build.gradle`, which is
originally imported from result of `yum list mesos`

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
$ RETZ_CONTAINER=`docker run -d -p 15050:15050 -p 15051:15051 -p 19090:19090 \
     --privileged \
     -v $(pwd)/build:/build mesos-retz`
```

If you want to map mesos workspace to host directory, you can add another option like
`-v $(pwd)/build/var:/var/lib/mesos`.

Spawn mesos slave and retz server and confirm they are alive.

```
$ docker exec -it $RETZ_CONTAINER sh -c "/spawn_mesos_agent.sh && sleep 1"
$ docker exec -it $RETZ_CONTAINER sh -c "/spawn_retz_server.sh && sleep 1"
$ docker exec -it $RETZ_CONTAINER ps awxx
```

Now it's possible to kick retz client at host OS.

```
% ./src/test/resources/retz-client load-app -F file:///spawn_retz_server.sh -A echo
% ./src/test/resources/retz-client list-app
$ ./src/test/resources/retz-client run -A echo -cmd 'echo foo' -R ./out
$ ./src/test/resources/retz-client unload-app -A echo
```

*Note* When you run with Docker for Mac, remove `-R ./out` option in `run` command.
Download will fail because no bridge interface like `docker0` is not installed.

To stop the container, it's better to add short timeout as `-t 1` or just kill it:

```
$ docker stop -t 1 $RETZ_CONTAINER
## or
$ docker kill $RETZ_CONTAINER
```

## Docker related messy things

- To enable cgroups, which is needed by mesos agents, systemd is installed
  and some special steps are necessary. For details, refer
  http://developers.redhat.com/blog/2014/05/05/running-systemd-within-docker-container/ .

- Now port mapping is assumed to be static for mesos master, agent and retz-server.
  When we want to run multiple mesos instances, e.g. for parallel test
  execution, there should be some trick to pass mesos master IP:port to
  external world and use it from test handle.

- Docker for Mac does NOT support tap/bridge device like `docker0` in
  Linux, at least, in latest version of 2016-08-19. Because of it,
  network communication from host to containers should be via mapped
  ports.

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

