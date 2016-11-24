================================
Getting Started With Retz Server
================================

This document is for Retz administrators, who operate Retz (and Mesos)
to manage and supply computing resources to `Retz users
<https://github.com/retz/retz/blob/master/doc/getting-started-client.rst>`_. This
document will cover:

* Prerequisites on setting up Mesos
* Prerequisites on setting up Retz
* Installing and setting up Mesos into single node
* Setting up Retz as an independent server process
* User management
* Operating and managing Retz service, upgrading
* Various options on setting up Retz
* High availability

Prerequisites on Mesos
======================

Retz heavily relies on `Apache Mesos <http://mesos.apache.org/>`_
. According to `getting stared page of Mesos
<http://mesos.apache.org/gettingstarted/>`_ , Mesos works with *Ubuntu
14.04, 16.04, CentOS 6.6 and 7.1* . Other operating systems are listed
there. But Linux is strongly recommended as Apache Mesos depends
heavily on Linux kernel related technology such as cgroups, App and
docker. Please refer to `official document
<http://mesos.apache.org/documentation/latest/>`_ for further details
and coverage.

For high availability in the sense of automatic fail-over of masters,
Mesos requires `Apache ZooKeeper <https://zookeeper.apache.org/>`_
. This document adopts single-node setup of ZooKeeper to cover
reasonable simplicity (and level of complexity) to set up Mesos. The
level of high availability relies on how ZooKeeper is set up, for
example, if ZK is set up with 3 nodes and 2 nodes mesos master, the
system endures only single node failure.

If you want to use *Docker* as container runtime in Mesos agents,
you'll need those systems installed in *ALL* agents. ( TODO: `AppC
<https://coreos.com/rkt/docs/latest/app-container.html>`_ is not yet
supported in Retz, OCI container format is not yet released, but will
follow )

If you have HDFS or any HDFS-compatible distributed file systems, and
want to fetch data from there, Hadoop ( ``hadoop`` command in the path)
is also required in Mesos agents.


Prerequisites on Retz
=====================

Retz itself works just with *Mesos >= 1.0.0 and Java 8*. It is tested
under latest Ubuntu Linux and CentOS, but also designed to work any
environment where JRE8 and Mesos Java client library works.

Even though Retz uses built-in database (H2) both on memory (default)
and on disk, if you want to save your users, applications and jobs in
more reliable storage, you can optionally choose `PostgreSQL
<https://www.postgresql.org/>`_ as external data store.

To manage Retz server process under systemd or initd, `supervisord
<http://supervisord.org/>`_ is strongly recommended::

  # apt-get install supervisor
  # systemctl start supervisor

Installing and setting up Mesos
===============================

This document uses Ubuntu 16.04 as an example. It is based on official
getting started page, which is based on Mesosphere's distribution of
Apache Mesos. First, set Mesosphere's key trust::

  # apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF

And tell Ubuntu APT Mesosphere's repository::

  # echo "deb http://repos.mesosphere.com/ubuntu xenial main" > /etc/apt/sources.list.d/mesosphere.list
  # apt-get update

The installation is quite simple::

  # apt-get install openjdk-8-jdk
  # apt-get install mesos
  # mesos-master --version
  mesos 1.0.1
  # mesos-agent --version
  mesos 1.0.1

To start all required process, run::

  # systemctl start zookeeper
  # systemctl start mesos-master
  # systemctl start mesos-slave

And then, try opening your IP address like http://10.0.2.5:5050 to see
whether Mesos master is running, and Mesos slave is properly registered
to Mesos master by opening "Agents" tab. There're many other ways to
see all those node are up or not:

* ``systemctl status [mesos-master|mesos-slave]`` to see process status.
  This does not work for ZooKeeper
* As ZooKeeper is started via nohup, ``netstat -an | grep 2181`` or
  event ``ps aux|grep java`` would help
* ``curl -i http://10.0.2.5:5050`` will tell wether Mesos master is up
  and listening
* ``curl -i http://10.0.2.5:5051`` will tell wether Mesos agent is up
  and listening
* Diagnose the system by reading logs with ``sudo journalctl -u mesos-master`` or ``sudo journalctl -u mesos-slave``
* Watching logs by ``sudo journalctl -f -u mesos-[master|slave]``

If you don't get Mesos processes successfully up, try

1. Explicitly telling Mesos your ZooKeeper address by writing ``zk://10.0.2.5:2181/mesos`` into ``/etc/mesos/zk``
2. Explicitly telling Mesos your IP address by writing ``10.0.2.5`` in  ``/etc/mesos-master/ip`` and ``/etc/mesos-slave/ip``
3. And restart both Mesos master and agents

If all those nodes are properly set up, then try first Mesos task
invocation with ``mesos-execute`` ::

  # mesos-execute --name=foobar --command="uname -a" --master=localhost:5050
  I1031 15:59:15.149842  3537 scheduler.cpp:172] Version: 1.0.1
  I1031 15:59:15.159924  3542 scheduler.cpp:461] New master detected at master@10.0.2.5:5050
  Subscribed with ID '2c9c9c02-a706-45ee-a7cc-85e8bd7afbd5-0002'
  Submitted task 'foobar' to agent '2c9c9c02-a706-45ee-a7cc-85e8bd7afbd5-S0'
  Received status update TASK_RUNNING for task 'foobar'
    source: SOURCE_EXECUTOR
  Received status update TASK_FINISHED for task 'foobar'
    message: 'Command exited with status 0'
    source: SOURCE_EXECUTOR

``TASK_FINISHED`` indicates your job success, or ``TASK_FAILED`` will tell
you something wrong, otherwise the command may seem hang if there are
no agents registered to master. Standard output and error output,
other files in a sandbox are provided at the task page in Mesos
console.

If ``mesos-execute`` blocks so long and even you have Mesos master and
agent running fine, you may have failed to set up their
connections. Or ``GPU_RESOURCES`` is required if you have agents set up
with GPUs ( ``gpu/nvidia`` in ``/etc/mesos-slave/isolators`` ).

Setting up Retz
===============

Download the latest DEB packages of ``retz-server`` and ``retz-admin``
from `GitHub release page <https://github.com/retz/retz/releases>`_
and install them. The server DEB creates ``retz`` user to run a Retz
process. This is an example of 0.0.29::

  # wget https://github.com/retz/retz/releases/download/0.0.29/retz-server_0.0.29_amd64.deb
  # wget https://github.com/retz/retz/releases/download/0.0.29/retz-admin_0.0.29_amd64.deb
  # md5sum retz-server_0.0.29_amd64.deb retz-admin_0.0.29_amd64.deb
  3f335c2db1ca50656e5d28303a78d91f  retz-server_0.0.29_amd64.deb
  1e539e086c45e113c7f832ffae8cdc75  retz-admin_0.0.29_amd64.deb
  # dpkg -i retz-server_0.0.29_amd64.deb
  # dpkg -i retz-admin_0.0.29_amd64.deb


Create a ``retz.properties`` file according to your environment. The deb
and rpm packages install an ``/opt/retz-server/etc/retz.properties``
file with default values, and this is also where the Retz server will
look for that file if not specified otherwise with the ``-C`` parameter.

The following options must be set in the ``retz.properties`` file:

* ``retz.mesos = 192.168.100.128:5050`` - A pair of IP address and port
  number where Mesos master is listening to. Thus Mesos master must be
  running
* ``retz.bind = http://localhost:9090`` - An URL of host name and port
  number where Retz will bind and start Web server (port number must
  be > 1024)
* ``retz.authentication = true`` - A flag whether Retz checks
  Authorization header in HTTP requests from clients.
* ``retz.access.key = deadbeef`` - Access key, and the identifier of a
  first user
* ``retz.access.secret = cafebabe`` - Secret key - change this to secure
  the system and never expose this to other people

Other settings are optional and documented in later part of this
document or in the `default configuration file
<https://github.com/retz/retz/blob/master/retz-server/src/main/dist/etc/retz.properties>`_
.

Retz is a program that runs just in foreground. To start Retz in
console, type

::

  # /opt/retz-server/bin/retz-server


And see it does not return, but just prints logs that indicate server
process successfully connects to Mesos and listens to port 9090.

You may also find Retz server started up by opening `the web console
<http://localhost:9090>`_ . If you want Retz server daemonized, use
external daemonization system like supervisord. Retz has example
supervisor configuration at
``/opt/retz-server/etc/retz-server.conf.supervisord-example``. To run
Retz under Supervisord::

  # adduser --system --group --no-create-home --disabled-login retz
  # cp /opt/retz-server/etc/retz-server.conf.supervisord-example /etc/supervisor/conf.d/retz-server.conf
  # systemctl restart supervisor
  # tail -f /var/log/retz-server.log

And see if Retz server successfully starts. Supervisord will also
manage log rotation and many other restarts.

(TODO: ``systemctl restart supervisor`` restarts all services under supervisord)

User management
===============

Retz is ready for managing multiple users and isolating them. To see
all available users, run::

  # /opt/retz-admin/bin/retz-admin list-user

And you'll see a complete list of existing users. To see further
details of each user::

  # /opt/retz-admin/bin/retz-admin get-user -id <userid>

To create a new user::

  # /opt/retz-admin/bin/retz-admin create-user


``create-user`` gives a new user's key id and secret to standard
output. They will be the pair of ``retz.access.key`` and
``retz.access.secret`` at clients configuration. Administrators must
provide users with ``retz.bind`` and ``retz.access.*`` at least.

Retz admin tool has a few more features.
To see them, try ``retz-admin help`` and ``retz-admin help -s <subcommand>``.

Operating and managing Retz service
===================================

Another option to run Retz, is to run it under Marathon
managemennt. (To be implemented and documented here)


Various options on setting up Retz
==================================

Mesos has many knobs to control its behaviour. See `Mesos
documentation
<http://mesos.apache.org/documentation/latest/configuration/>`_ for
complete list. Here is listed major use cases with Retz. These are all
optional for Retz, but strongly recommended.

* ``/etc/mesos-agent/isolation`` - A list of isolator
  definitions. ``docker/runtime`` and ``filesystem/linux`` is imporant to
  mount docker images and docker volume
  drivers. ``cgroups/cpu,cgroups/mem`` means cgroups is used to isolate
  CPU and memory between tasks under same agent. ``cgroups/devices`` is
  used with ``gpu/nvidia`` to show GPU devices on Mesos containerizers.
* ``/etc/mesos-agent/image_providers`` - Define container image
  providers. If you use Docker as Retz applictaion environment, just
  write ``docker`` to this file.
* ``/etc/mesos-agent/cgroups_enable_cfs`` - A flag to set hard limit to
  cgroup isolators. if ``cgrous/mem`` is set and this is true, OOM
  killer will kill your task once it exceeds the memory size of the
  task.



Also, Retz has many knobs to control its setup

* ``retz.mesos.principal = retz`` - Mesos principal name
* ``retz.mesos.role = retz`` - Role name in Mesos
* ``retz.mesos.secret.file`` - A file path containing mesos
  authentication secret (optional, no line breaks allowed in the file)
* ``retz.max.running = 128`` - A maximum number of simaltenous jobs that
  run under single Retz queue. This is to limit Retz usage of whole
  Mesos cluster.
* ``retz.max.stock = 16`` - A maximum number of resource offers to be
  kept in Retz after they are offered from Mesos. This will improve
  job execution latency on the cluster with light load. To disable
  stocking, explicitly set this to 0.
* ``retz.max.cpus = 8`` - Maximum number of CPUs per single job
* ``retz.max.mem = 31744``  - Maximum size of RAM per single job in MBs
* ``retz.max.gpus = 0`` Set maximum number of GPUs per single job - If
   your Mesos agent clusters has GPUs and you want to assign GPUs to
   your task, set this to 1 or more.
* ``retz.max.disk = 1024``  - Maximum size of disk usage per single job in MBs

Database configurations - by default Retz stores all information on
memory.

* ``retz.database.url`` A JDBC address where Retz connects. Default is
  ``jdbc:h2:mem:retz-server;DB_CLOSE_DELAY=-1``. To store data
  persistently on disk (file ``/var/run/retz.db``), use
  ``jdbc:h2:file:/var/run/retz.db``. PostgreSQL example:
  ``jdbc:postgresql://127.0.0.1:5432/retz``
* ``retz.database.driver`` - A JDBC driver name; ``org.h2.Driver`` for H2 and ``org.postgresql.Driver`` for PostgreSQL.
* ``retz.database.user``
* ``retz.database.pass``

Theoretically as all of these does not depend on specific
implementation, if you pass proper JDBC implementation to Retz and set
these properly Retz work any relational databases that supports JDBC.

These configurations are all about SSL on Retz client-server
communitation, which is used only when ``retz.bind`` address has
``https`` scheme.

* ``retz.tls.keystore.file``
* ``retz.tls.keystore.pass``
* ``retz.tls.truststore.file``
* ``retz.tls.truststore.pass``
* ``retz.tls.insecure = false``

High availability
=================

Currently Retz does not have any high availability features, except
storing data into persistent database. Operators might be able to set
up highly available PostgreSQL instance and tie Retz to it.


Build from source
=================

Retz needs JDK8 to build.

::

  $ git clone https://github.com/retz/retz
  $ cd retz
  $ make deb

or run ``make rpm`` for RPM-managed environment like Red Hat, Fedora,
CentOS Linux. To run without installation, just run

::

  $ bin/retz-server

at the cloned directory.

It is also possible to run ``make server-jar`` to obtain a jar file with
all dependencies bundled. To run Retz server with a jar file::

  # java -jar ./retz-server/build/libs/retz-server-x.y.z-all.jar -C retz.properties

for the jar version in your repository.
