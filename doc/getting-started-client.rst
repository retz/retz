================================
Getting Started With Retz Client
================================

This document is for Retz users, especially utilizing Retz as a batch
execution service. All they need is their task definition, Retz client
and JRE. Please go to `Getting Started With Retz Server
<https://github.com/retz/retz/blob/master/doc/getting-started-server.rst>`_ for server
operation and administration. This document will cover:

* Prerequisites on offcial client
* Installation and configuration
* Hello, Retz
* Defining your application environment
* Running your commands
* Watching your jobs
* Fetching results to local

Prerequisites on offcial client
===============================

You will need Oracle JRE 8 to run Retz clients. Supported OSs are,

* Ubuntu 14.04 or later, Or Debian 8 or later
* CentOS 7.2 or later
* MacOS 10.11 or later

And please make sure that you have Java Runtime > 1.8

::

   $ java -version
   java version "1.8.0_77"
   Java(TM) SE Runtime Environment (build 1.8.0_77-b03)
   Java HotSpot(TM) 64-Bit Server VM (build 25.77-b03, mixed mode)

Installation and configuration
==============================

Go visit `Retz release page <https://github.com/retz/retz/releases>`_
and find out release that matches server version. *For 0.x.y server,
clients should be 0.x.z*. Although exact version must be
provided by server administrator, to find correct version just hit
curl like ``curl -i http://retz.example.com:9090/ping`` and see
``Server:`` header.

For 0.1.0 as a package installation example,

* For Ubuntu or Debian, download `Debian package
  <https://github.com/retz/retz/releases/download/0.1.0/retz-client_0.1.0_amd64.deb>`_
  and install via ``dpkg -i``.
* For CentOS, download `RPM package
  <https://github.com/retz/retz/releases/download/0.1.0/retz-client-0.1.0-1.el7.x86_64.rpm>`_
  and install via ``rpm -i``.

Both ways install a command ``/opt/retz-client/bin/retz-client`` and a
configuration file ``/opt/retz-client/etc/retz.properties`` . Include
``/opt/retz-client/bin`` to ``$PATH`` environment by running
``export PATH=/opt/retz-client/bin:$PATH`` . In later part of this document,
they will be called respectively ``retz-client`` and ``retz.properties`` .

To install with just a jar file, download `the fat jar file
<https://github.com/retz/retz/releases/download/0.1.0/retz-client-0.1.0-all.jar>`_
. And place it wherever you like, and create a shell script like

::

   #!/bin/sh
   set -e
   java -jar /path/to/lib/retz-client-0.1.0-all.jar -C /path/to/etc/retz.properties "$@"

And then install it to a path that is included in ``$PATH`` . Also,
install ``retz.properties`` to ``/path/to/etc`` . These shell script and
configuration file are respectively noted as ``retz-client`` and
``retz.properties`` in later part of this document.


Editng configuration file: basically, client configuration file must
be provided by the server administrator. Refer to `README
<https://github.com/retz/retz/blob/master/README.md>`_ for details -
just important items, which must be provided by service administrator
are being listed:

* ``retz.server.uri`` - e.g. ``http://retz.example.com:9090`` in later part of this document
* ``retz.authentication``
* ``retz.access.key`` - when authentication is true
* ``retz.access.secret`` - when authentication is true

Hello, Retz
===========

To see server availability, run curl like this:

::

   $ curl http://retz.example.com:9090/ping

If you get ``OK`` then the server is running HTTP service correctly. Also, try typing

::

   $ curl http://retz.example.com:9090/status

to see immediate running status of the server.


To check client connectivity, try

::

   $ retz-client list-app

And you'll see all applications. Next, choose a unique name and try
registering your application by

::

   $ retz-client load-app -A your-test-app
   ..
   $ retz-client get-app -A your-test-app
   ..
   I15:37:49 ok
   I15:37:49 Application name=your-test-app:  container=MesosContainer: files=/:

Then, run

::

   $ retz-client run -A your-test-app -cmd "echo 'hello, retz'"

This may take time, but finally you'll get:

::

   I15:40:15 job 11 scheduled
   I15:40:47 job 11 started: STARTING
   I15:40:47 ============ stdout-11 in job 11 sandbox start ===========
   hello, retz
   I15:40:48 ============ stdout-11 of job 11 sandbox end ===========
   I15:40:48 FINISHED 2016-09-26T03:40:39.881+09:00
   I15:40:48 Job(id=11, cmd='echo 'hello, retz'') finished in 0.364 seconds and returned 0


Defining your application environment
=====================================

All application environment is to be defined via ``load-app`` command.

* ``--container [docker|mesos]`` which container to use. Default is mesos.
* ``--image`` When using Docker container, set image name like ``ubuntu:16.04``
* ``--user`` A user name to run commands with. Default is system-setup specific.
* ``-F`` An URL for Mesos agent to download before invoking
  task. Protocols accepted are: ``http``, ``https`` and ``hdfs``.
  Compressed files will be unarchived to ``$MESOS_SANDBOX`` .
* ``-L`` An URL for Mesos agent to download and cache locally
* ``-P`` An URL for Retz executor to download and cache data at `Mesos
  persistent volumes
  <http://mesos.apache.org/documentation/latest/persistent-volume/>`_
  . Its volume size must be specified with ``-disk`` option.

Example:

::

   $ retz-client load-app -A docker-app-example --container docker \
      --image ubuntu:16.04 -L http://example.com/your-app.tar.gz \
      -L hdfs://example.com/path/to/your-data.tar.gz \
      -F hdfs://example.com/path/to/your-frequently-changed-data.tar.gz

Running your commands
=====================

This should be what you want; you have two subcommand choices to run your job:

* ``run`` - Run and watch your job as if you're running locally and synchronously
* ``schedule`` - Submit your job and let it go; you may get its status or result anytime you want later

Both ``run`` and ``schedule`` have same options:

* ``-A`` - tell your application name where you want to run the command
* ``-E`` - set environment variable at runtime
* ``-cpu``, ``-mem``, ``-gpu`` - numbers of resources you want. CPU is for
  number of cores (default is 1), memory is for RAM size in MBs
  (default is 32), GPU is for numbers of GPUs to be visible at
  container (default is 0 [#]_ ).
* ``-cmd`` - set command one liner - shell variables are to be evaluated.

.. [#] Whether GPU is available or not depends on system setup,
       which information should be provided by system administrator.

Example run::

  $ retz-client run -A your-app -E 'YOUR_APP_ENV=-Xmx65536m' \
    -cmd 'your-app-cmd -thread 16' -cpu 16 -mem 65536

This command blocks until Retz accepts the job, wait for resource
offer from Mesos, environment fetch, command invocation and its finish
(or kill) and prints *only standard output* of the command. ``run`` is
*essentially equal* to combination of ``schedule`` and ``get-file --poll`` .

Note that you can't stop remote command just by sending SIGTERM or
SIGKILL to Retz client process. Instead ``kill`` subcommand are
available, which sends SIGTERM to your command (or Docker container).

Or example schedule::

  $ retz-client schedule -A your-app -E 'YOUR_APP_ENV=-Xmx65536m' \
    -cmd 'your-app-cmd -thread 16' -cpu 16 -mem 65536

Watching your job status
========================

In addition to just ``run``, There are three ways to see job status
depending on the purpose. ``list`` is to list statuses *all* jobs,
``get-job`` is to get simple summary of status of the job and ``get-file``
is to get file in the sandbox. Major use cases are ``list`` to see
overview of all jobs, and ``get-file`` with ``--poll`` to watch job
progress.

Fetching results to local
=========================

After the job finished, ``get-file`` and ``list-files`` are ways to get
results of the jobs. Each is respectively like ``get`` and ``ls`` in FTP
interactive shell - gets file and lists files in the sandbox. Check
out job id and give it to see any information:

::

   $ retz-client list
   ...
   I16:51:26 TaskId State    AppName       Command            Result Duration Scheduled                     Started                       Finished                      Reason   ...
   I16:51:26 11     FINISHED test          echo 'hello, retz' 0      0.364    2016-09-26T03:40:15.573+09:00 2016-09-26T03:40:39.517+09:00 2016-09-26T03:40:39.881+09:00 -
   $ retz-client list-files -id 11
   I16:53:05 gid     mode       uid  mtime               size     path
   I16:53:05 nogroup -rw-r--r-- retz 2016-40-26 03:40:23 1878     stderr
   I16:53:05 nogroup -rw-r--r-- retz 2016-40-26 03:40:37 0        stderr-11
   I16:53:05 nogroup -rw-r--r-- retz 2016-40-26 03:40:39 4041     stdout
   I16:53:05 nogroup -rw-r--r-- retz 2016-40-26 03:40:38 12       stdout-11

Then get some files:

::

   $ retz-client get-file -id 11 --fetch stdout-11 -R path/to/result/dir

A file named ``stdout-11`` should be created at ``path/to/result/dir``
. Without ``-R`` option, the file is printed out to standard output.
If your job had failed, getting ``stderr`` or ``stderr-11`` in this case
may help you diagnose problem, but usually printed reasons are
Mesos-specific. Asking to the administrator each time would highly
recommended at first trial.

Further Resources
=================

- Type ``retz-client help`` or ``retz-client help -s <subcommand>`` to see
  all specs of client command.
- For Java programming API, `io.github.retz.web.Client
  <https://github.com/retz/retz/blob/master/retz-client/src/main/java/io/github/retz/web/Client.java>`_
  is official API for Java programmers to hack.
- There are no documentation of RESTful API, but
  `io.github.retz.protocol
  <https://github.com/retz/retz/tree/master/retz-common/src/main/java/io/github/retz/protocol>`_
  is JSON protocol definition and `io.github.retz.auth
  <https://github.com/retz/retz/tree/master/retz-common/src/main/java/io/github/retz/auth>`_
  is authentication implementation.
- `Retz Official Site <http://retz.github.io>`_
- `Retz Release Downloads <https://github.com/retz/retz/releases>`_
- `Retz Release Notes <https://github.com/retz/retz/blob/master/doc/RELEASE-NOTES.md>`_
- `Report Issues <https://github.com/retz/retz/issues>`_
