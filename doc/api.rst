Commands, Client API and Admin tool
===================================

This document describes Retz client CLI, Java API and HTTP API as well
as server command options and administration tool. All packages are
available at `GitHub release page
<https://github.com/retz/retz/releases>`_ . Each release has client,
server and admin tool packages for CentOS 7, Redhat 7, Debian and
Ubuntu 16.04 (Linux distributions that has Systemd and RPM or Deb
based package manager).

Retz adminitration tool is also described.

Libraries
---------

Client library is available via Maven repository. For example,
dependency description in Gradle will be as follows:

::
  apply plugin: 'java'

  repositories {
      maven { url 'http://retz-maven.s3.amazonaws.com/releases' }
      maven { url 'http://retz-maven.s3.amazonaws.com/snapshots' }
  }

  dependencies {
    compile 'org.slf4j:slf4j-api:1.7.21'
    compile group: 'io.github.retz', name: 'retz-client', version: '0.2.5'
  }




Retz Client
-----------

Retz client CLI consists of several subcommands that corresponds
mostly 1:1 to HTTP/JSON API, as well as Java API.

Client configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~

Client configuration file must be written in `Java Properties style
format
<https://docs.oracle.com/javase/tutorial/essential/environment/properties.html>`_
and parsed with ``java.util.Properties`` .


``retz.server.uri = http://10.0.0.1:5050``

   Defines Retz server location to send all requests.

``retz.access.key = cafebabe``

   Defines user identity to send requests to servers with.

``retz.access.secret = deadbeef``

   Defines access secret to identify and authenticate a user.


Authorization and authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All HTTP requests that requires authentication must have
``Authorization`` header with following form::

  Authorization: Retz-auth-v1 <AccessKey>:<Signature>

``<AccessKey>`` must be given to users by system administrator.
``<Signature>`` must be generated as follows::

    Signature = Base64( HMAC-SHA1( YourAccessSecret, UTF-8-Encoding-Of( StringToSign ) ) );

    StringToSign = HTTP-Verb + "\n" +
                   Content-MD5 + "\n" +
                   Date + "\n" +
                   Resource;

    Resource = [ /job/<job-id> | /app/<appname> | /jobs | /apps | /u/<accesskey> | ... ]

Note that ``Resource`` does not include parameters like ``?a=b``, or
``#foobar``.

Client CLI and API
~~~~~~~~~~~~~~~~~~

``retz-client [-C|--config <path/to/retz.properties>] [-v|--verbose] <subcommand> OPTIONS``

   Starts Retz client with a configuration file described above.
   With installer, ``retz-client`` will be installed to ``/opt/retz-client/bin``.

``retz-client help``

   Prints help and lists all subcommands.

* ``-s <subcommand>`` :   Prints help of each subcommand with its options.

``retz-client config``

   Test and prints configuration items written in the configuration file locally.

``retz-client list OPTIONS``

   Lists all jobs that belongs to the user. This uses ``GET /jobs``
   HTTP endpoint with empty body. Note that max number of jobs returned by server
   in a single HTTP request is limited by ``retz.max.list-jobs`` at server.
   `Request
   <https://retz.github.io/javadoc/io/github/retz/protocol/ListJobRequest.html>`_
   and `Response
   <https://retz.github.io/javadoc/io/github/retz/protocol/ListJobResponse.html>`_
   in Java API. ``OPTIONS`` include one of following:

* ``--state STATE`` Fetch all jobs in the state
* ``--states [STATE]`` Fetch all jobs in any of the states. Default is ``QUEUED,STARTING,STARTED``.
  ``ALL`` stands for all states. Will be ignored when '--state' is used.
* ``--tag TAG`` Fetch only jobs that has the tag, from those that has the
  state specified by ``--state`` or ``--states`` .

``retz-client schedule OPTIONS``

Schedules a job to Retz server. The command returns immidiately after
the job is put into the queue and will be executed eventually. If the
queue was empty and have a sufficient and stocked resource offer, Retz
server executes the job immidiately. If a job is successfully
scheduled, it prints job id number to console, which is required in
``kill``, ``get-job``, ``list-files`` and ``get-job``. Job id can also
be found in the result of ``list``.

This uses ``POST /job`` endpoint with a job spec written in JSON in
the body.  `Request
<https://retz.github.io/javadoc/io/github/retz/protocol/ScheduleRequest.html>`_
and `Response
<https://retz.github.io/javadoc/io/github/retz/protocol/ScheduleResponse.html>`_
in Java API


``OPTIONS`` follows:

* ``-A <appname>`` : Defines an application name (required)
* ``-c|--command <commandline>``:   Defines command line to run (required)
* ``-E env=value`` : Defines environment values. ``$HOME`` and
  ``$MESOS_*`` are overwritten by Mesos executor.
* ``--cpu <int>``        :   Defines number of CPUs required for the job (default: 1)
* ``--mem <int>``        :   Defines amount of RAM in MiB required for the job (default: 32MB)
* ``--ports <int>``      :   Defines number of IP ports required for the job (default: 0)
* ``--gpu <int>``        :   Defines number of GPUs required for the job (default: 0)
* ``--prio|--priority <int>``       :   Defines job priority (default: 0). Priority handling depends on server planner setting.
* ``-N|--name <name>``  :   Defines human readable job name.
* ``--tags <tag>[,<tag>] : mark the job with tags

``retz-client run OPTIONS``

Schedules a job to Retz server and waits for it finish either
successfully or not. Return value is ``0`` if the job finishes in
``TASK_FINISHED`` at Mesos. In addition to same options as
``schedule``, ``--stderr`` is available.

This command is implemented with combination of ``ScheduleRequest``
and ``GetFileRequest``.

* ``--stderr`` : Prints stderr after the job finished to standard error when this option is specified.
* ``--timeout`` : Timeout in minutes. After timeout, the client tries to kill the job.


``retz-client get-job -i <id>``

Fetches and prints a job spec and status from Retz server.

This uses ``GET /job/<id>`` endpoint with empty body.  `Request
<https://retz.github.io/javadoc/io/github/retz/protocol/GetJobRequest.html>`_
and `Response
<https://retz.github.io/javadoc/io/github/retz/protocol/GetJobResponse.html>`_

``retz-client get-file -i <id> OPTIONS``

Fetches a file from job sandbox and outputs to a path specified with
``-R``.

This uses ``GET /job/<id>/file?path=<path>&offset=<offset>&length=<length>`` endpoint
with empty body.
`Request <https://retz.github.io/javadoc/io/github/retz/protocol/GetFileRequest.html>`_
and
`Response <https://retz.github.io/javadoc/io/github/retz/protocol/GetFileResponse.html>`_

* ``--path <path>``: Defines a file to get (default: ``stdout``)
* ``--poll``: If the job is not finished, wait for the job to finish (default: ``false``)
* ``[-R|--resultdir] [<path>|-]`` : Define a directory to output in local (default: standard output)
* ``--offset <offset>``: Define offset to start fetch with (default: 0)
* ``--length <length>`` : Define length to fetch (default: -1; get the whole file)
* ``--binary`` : Specify the file as binary file. This should be with ``--resultdir`` option
  and without ``--poll`` option. ``--offset`` and ``--length`` will be ignored.
* ``--timeout`` : Timeout in minutes. After timeout, the client tries to kill the job.
  This is available with ``--poll`` option on.

.. note:: With ``--binary`` option specified, Retz client uses ``GET /job/<id>/download?path=<path>``
          endpoint with empty body, resulting raw binary data in HTTP response body.

``retz-client list-files -i <id>``

List files in a directory in sandbox. This uses ``GET /job/<id>/dir?path=<path>`` endpoint with empty body.
`Request
<https://retz.github.io/javadoc/io/github/retz/protocol/ListFilesRequest.html>`_
and `Response
<https://retz.github.io/java/doc/io/github/retz/protocol/ListFilesResponse.html>`_

.. note:: If the path is just file name in sandbox, Retz client
          replaces it with ``$MESOS_SANDBOX`` to avoid empty
          parameter.

.. note:: ``list-files`` is a bit different from popular ls(1) from Unix
          when listing exact one file. In ls(1) it does print full
          path or full metadata, but Retz's ``list-files`` does not show
          anything, as there is no such file. To make sure that the
          file exists, run ``list-files`` against parent directory.

``retz-client kill -i <id>``

Kills a job, even if it is already running in Mesos agent. When the
job is still in the queue, Retz changes the state from ``QUEUED`` to
``KILLED``. If the job is already running at remote, Retz tries to
kill it with `MesosSchedulerDriver#killTask
<http://mesos.apache.org/api/latest/java/org/apache/mesos/MesosSchedulerDriver.html#killTask(org.apache.mesos.Protos.TaskID)>`_
.

This uses ``DELETE /job/<id>`` API endpoint with empty body. `Request
<https://retz.github.io/javadoc/io/github/retz/protocol/KillRequest.html>`_
and `Response
<https://retz.github.io/java/doc/io/github/retz/protocol/KillResponse.html>`_

``retz-client killall --tag <tag>``

Kills a group of jobs, even if it is already running in Mesos agent. When the
job is still in the queue, Retz changes the state from ``QUEUED`` to
``KILLED``. If the job is already running at remote. Currently this is a wrapper
of 'list' and 'kill'.

``retz-client get-app -A <appname>``

Gets all of application information via ``GET /app/<appname>`` with
empty body, which returns a JSON in body.

`Request
<https://retz.github.io/javadoc/io/github/retz/protocol/GetAppRequest.html>`_
and `Response
<https://retz.github.io/java/doc/io/github/retz/protocol/GetAppResponse.html>`_

``retz-client list-app``

Lists all applications owned by the user, via ``GET /apps``.

`Request <https://retz.github.io/javadoc/io/github/retz/protocol/ListAppRequest.html>`_ and `Response <https://retz.github.io/java/doc/io/github/retz/protocol/ListAppResponse.html>`_

``retz-client load-app OPTIONS``

Registers a new application or overwrites an existing application with
a new specification.

This uses ``PUT /app/<appname>`` with an application defined in JSON
in body. `Request
<https://retz.github.io/javadoc/io/github/retz/protocol/LoadAppRequest.html>`_
and `Response
<https://retz.github.io/java/doc/io/github/retz/protocol/LoadAppResponse.html>`_

Options follows:

* ``-A|--appname <appname>`` : Define an unique name of an application (required)
* ``-U|--user <username`` : Specify an unix user name who runs the
  task in agents (default: a user name that runs Retz server).
* ``--container [mesos|docker]`` : Specify image type of `Mesos
  Containerizer
  <http://mesos.apache.org/documentation/latest/container-image/>`_
  . (default: mesos)
* ``--image`` : Specify a container image name (required when using
  docker image). Private registry is also available, with same `naming
  rule <https://docs.docker.com/registry/introduction/>`_ defined by
  Docker ( see also `Deploying a registry server
  <https://docs.docker.com/registry/deploying/>`_ .
* ``--docker-volumes`` : Specify a volume name `to mount in docker
  container
  <https://docs.docker.com/engine/tutorials/dockervolumes/>`_
  . Volumes are mounted in sandbox.
* ``-F|--file <URI>`` : File URIs to pass to `Mesos Fetcher
  <http://mesos.apache.org/documentation/latest/fetcher/>`_ before a
  job starts.
* ``-L|--large-file <URI>`` : Same as ``--file`` , but Mesos agents
  where the task launched cache the file locally and prevents
  downloading again at next time it runs same task.
* ``--enabled`` : with ``false`` specified, the application is
  disabled and cannot be used for job invocation.
* ``--grace-period`` : Defines KillPolicy to each task that belongs
  to the application with duration in seconds. Default value is 0,
  whose meaning is "setting no KillPolicy and adopt Mesos default
  behaviour".

Other HTTP endpoints
~~~~~~~~~~~~~~~~~~~~

Retz server also works as HTTP server supporting following endpoints:

* ``/`` : An URL that provides human readable web page for browsers.
* ``/ping`` : A monitoring check URL for Java clients and simple HTTP
  clients like cURL. `Client#ping()` is a method for this.
* ``/status`` : A health check URL for Java clients. It returns
  `StatusResponse
  <https://retz.github.io/javadoc/io/github/retz/protocol/StatusResponse.html>`_
  JSON in body.

These endpoints do not require authorization.

Retz Server
-----------


``retz-server [-C|--config <path/to/retz.properties>] [-M|--mode local|mesos]``


Starts Retz server, writing logs out to standard output.
With installer, ``retz-server`` will be installed to ``/opt/retz-server/bin``.


* ``-C </opt/retz-server/etc/retz.properties>``: Specify configuration
  file location.
* ``--config </opt/retz-server/etc/retz.properties>``: Syntax sugar of
  ``-C`` .
* ``-M [local|mesos]`` : Scheduler mode. It is to connect to Mesos
   master.  ``local`` is to test Retz HTTP/JSON API without connecting
   to Mesos (default value: ``mesos``)
* ``--mode [local|mesos]``: Syntax sugar of ``-M`` .

Optionally Retz can be started with just Java command fat jar file (
e.g. ``retz-server-0.0.33-all.jar`` ), as follows:

.. code-block:: sh

   java -jar path/to/retz-server-0.0.33-all.jar -C path/to/retz.properties



Server configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~


* ``retz.mesos = localhost:5050``:   Mesos host name and port. (required)
* ``retz.mesos.role = retz`` : Set `a Mesos role <http://mesos.apache.org/documentation/latest/roles/>`_ name to
   register as a framework. If this is not specified, principal value
   is used for role name, too.
* ``retz.mesos.principal = retz``: Set `a Mesos principal <http://mesos.apache.org/documentation/latest/authorization/>`_
   name. Default value is ``retz``.
* ``retz.mesos.secret.file = path/to/secret-file``: If `authentication in Mesos
   <http://mesos.apache.org/documentation/latest/authentication/>`_ is
   enabled, set a file name that has secret to access Mesos.
* ``retz.mesos.refuse = 3`` : (sort of) interval of resource offer from Mesos
* ``retz.bind = http://localhost:9090``: A URL and port number to
   listen. If the scheme is ``https`` Retz tries to serve as an HTTPS
   server with keys defined with ``retz.tls.*`` properties. This value
   **must** match with ``retz.server.uri`` in clients' configuration.

   Although the default address is ``localhost``, it is recommended to
   use IP address that is accessible from external nodes.

* ``retz.authentication = true``:   Enable authentication between client and server. If this is false,
   Retz server does no verification and authentication on server side.
   (``retz.access.key`` is still required in client configuration to
   identify job and application owner)
* ``retz.access.key = deadbeef``:    Define first user's key
* ``retz.access.secret = cafebabe``:    Define first user's secret
* ``retz.max.running = 128``:    Limit of simultaneous job execution
* ``retz.max.stock = 16``:
* ``retz.max.cpus = 8``: Max size of a job (memory and disk are in MBs)
* ``retz.max.mem = 31744``
* ``retz.max.gpus = 0``: Sets GPU_RESOURCES aas GPU-enabled framework when max.gpus > 0
* ``retz.max.ports = 10``
* ``retz.max.disk = 1024``
* ``retz.max.list-jobs = 65536`` Max number of jobs that is allowed to retrieve from client

* ``retz.database.url = jdbc:h2:mem:retz-server;DB_CLOSE_DELAY=-1`` : JDBC access URL
* ``retz.database.driver = org.h2.Driver`` : JDBC Driver name
* ``retz.database.user =`` : Database access user name
* ``retz.database.pass =`` : Database access passwoord

* ``retz.tls.keystore.file =``
* ``retz.tls.keystore.pass =``
* ``retz.tls.truststore.file =``
* ``retz.tls.truststore.pass =``
* ``retz.tls.insecure = false``

* ``retz.planner.name = naive`` : Planner setting. Must be either "naive" or "priority".
* ``retz.jmx.port = 9999`` : JMX port number to listen on. Note that server only binds
  loopback address.

* ``retz.gc = true`` : A knob to turn off automatic job garbage collection
* ``retz.gc.leeway = 7 * 86400`` : Leeway seconds where finished/killed jobs are deleted
* ``retz.gc.interval = 600`` : Interval in seconds that old job garbage collection
  process is invoked

Planner choice

naive
  Setting ``naive`` is default option; server sets up ``NaivePlanner`` as planner object.
  Job sorting is based on job IDs, which is sequencially generated. With this, jobs are
  executed on scheduled time order.
priority
  Setting ``priority`` lets server set up ``PriorityPlanner`` as planner object. With
  priority planner, Retz chooses a job based on priority, and next scheduled time order.
  The number or priority can be given by ``-prio`` option at CLI for example. It must
  range from -20 to 19, defaults to 0. Altough the range is like Unix nice, but it is
  different as Unix nice is used for timeslice. Retz's priority planner uses this value
  just for ordering of jobs like IDs.


Retz Administartion Tool
------------------------

``retz-admin`` is an administration tool that supports
``create-user``, ``disable-user``, ``enable-user``, ``list-user`` and
``usage``. With installer, ``retz-admin`` will be installed to
``/opt/retz-admin/bin``.

Administration tool shares configuration file with server (or give it by ``-C`` option
as well) to see common ``retz.jmx.port``.
