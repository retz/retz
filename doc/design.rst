Retz: A Job Scheduler for Batch Processing
==========================================

Asakusa on M3BP is a compiler and DAG processing engine that executes
batch by multithreading in single node to omit the overhead of
distributed processing. Spark and Hadoop may process batches in
parallel in users' production environments even in areas that have
not-so-big data and low investment effectiveness. However, in an
actual environment, there are many daily or medium-scale daily or
monthly processing, and existing job management systems can not
schedule this successfully.

Overall system usually runs many batch jobs in many nodes. Retz is a
job scheduler that runs Asakusa on M3BP jobs most efficiently - at
first it was designed business batch programs like Asakusa on M3BP, it
is also applicable to other CPU/memory intensive batch programs like
machine learning, multimedia processing. Such use cases generally have
following requirements.

- Separate resource usage such as CPU and memory among tasks
- All nodes are similar, transparent and fair to start any jobs
- Automatic re-run in case of system failures
- Even in the degenerate environment due to node failure, all
  processes must be processed in order rather than discarding them
- Input jobs at arbitrary timing and process them at the time when resources can be secured
- Jobs should be executed in ascending order, or in possibly optimized order
- Be able to view, kill and re-run waiting jobs
- The system as a whole maintains the ideal resource utilization rate (80% to 95%)
- Optimization in resource usage efficiency
  

Architecture and components
---------------------------

Retz is based on `Apache Mesos <http://mesos.apache.org>`_ as a
resource management system of multiple nodes. The functionality of
Mesos is:

- Monitoring agents, managing their resources and deploying tasks
- State management of running job ("task" in Mesos terminology)
- Assignment of resources to the framework (managing applications using Mesos ...)
  
In addition to this, `Marathon
<https://mesosphere.github.io/marathon/>`_ that manages other daemon
processes that you want to run on Mesos, and `Chronos
<https://mesos.github.io/chronos/>`_ may optionally be added to the
system. These systems can be operated in high availability mode using
ZooKeeper (* Mesos will work without ZK, but Marathon and Chronos need
ZK). By using ZooKeeper in a high-availability configuration, the
system can continue operation without inconsistency even if network
disruption occurs.

Retz is developed as a framework to manage general single node batch
jobs like Asakusa on M3BP. Retz does

- Job execution and queuing (on memory, on disk, or on other RDBMS)
- Manages references to files and container images for the batch program
  
The figure below is a typical Retz set up with a highly available
configuration. When high availability is unnecessary and you can
tolerate re-execution of downtime or batch, you can omit ZooKeeper and
Marathon (you can just start with ZooKeeper in single-node
configuration).


::

   +----------------------+
   | on-M3BP-Job <- Agent-+----ZK------------+
   +------------------^---+    |             |
                      |    +---+----+   +----+-----+
                      +----| Master |---| Marathon |
                           +---^----+   +----+-----+
                               |             |
     Operator ------------>  Retz <----------+
                               ^
                               |
      User ---(submit a job)---+
 
   --> manage
   --- connected


Retz server process also could be managed by Marathon and node failure
would also be failed over automatically. But persistent data like
users, applications and jobs must be recovered from old snapshots, or
database (PostgreSQL would be the first choice) behind must configured
as highly available.

Retz server
-----------


```sh
# /opt/retz-server/bin/retz-server
```

The server requires correct address of Mesos master - otherwise
it cannot start as a Mesos framework scheduler.

The server runs in foreground, unlike traditional daemon process.
This is partly for simplicity, and partly for Marathon. Daemonization
tools such as [Supervisor](http://supervisord.org) will help you run
Retz in background. Retz server package includes supervisord
configuration sample in `/opt/retz-server/etc`.


Retz client
-----------

Users access Retz server via Retz API. It is HTTP/JSON API with simple
authentication. Java client library and CLI tool is avialble.


Typical use case is,

0. Get an account (a pair of access key and secret) from Retz server
   administrator, writing it down to
   `/opt/retz-client/etc/retz.properties``
1. Register application via ``load-app``, specifying application
   files, data sources or container images
2. Make sure your application is successfully set up in Retz server
   with ``list-app`` or ``get-app``
3. Run any command with ``run``, or schedule a long running job with ``schedule``
4. Check status with ``get-job`` or ``list`` command, or watch any file with ``get-file --poll``
5. When it finished, browse and get results using ``list-files`` and ``get-file``


All subcommands have corresponding Java API, so use cases like this
are all programmable. `io.github.retz.web.Client
<https://retz.github.io/javadoc/io/github/retz/web/Client.html>`_ is
the main API of client library.

.. note:: As Retz is still in early stage and API is unstable,
          `Javadoc <https://retz.github.io/javadoc/>`_ may not be
          latest or up to date. It is recommended to watch release
          notes and build Javadoc yourself.



How to deploy applciations
~~~~~~~~~~~~~~~~~~~~~~~~~~

Before job invocation, execution program ('deployment archive' in
terms of Asakusa) must be deployed in task sandbox. For Mesos agents
to fetch and prepare it locally for sandbox, it must be available via
network - such as via HTTP service, HDFS filesystem, or network file
system mounted in all agents. ``load-app`` defines such environment.

``load-app`` can also define Docker container image and its registry.
AppC is supported by Mesos, but not yet tested. OCI is to be supported
by Mesos, possibly followed by Retz.

Job scheduling and execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both ``schedule`` and ``run`` submit a job to the job queue in Retz
server. ``schedule`` returns just after submission, while ``run``
command keeps polling and printing standard output of the job, until
it finishes.::

  retz-client run -A your-java-app \
    -cmd 'git clone git://github.com/retz/retz && make -C ./retz build'\
    -cpu 4 -mem 8196


Both commands allows arbitrary command that is available in agent, or
available in the container image.


Job retry
~~~~~~~~~

In any pattern of task failure, Mesos simply notifies the result to
the framework by callback and does no retry. Retz will be notified of
the failure and try again several times.

.. note:: In current implementation (~0.0.33) Retz retries only in
          case of ``TASK_LOST``. In future, Retz will retry several
          times in case of ``TASK_ERROR``, ``TASK_FAILED`` and
          ``TASK_KILLED`` and the number of job retry will be
          configurable via CLI/API.

Collecting Job Results
~~~~~~~~~~~~~~~~~~~~~~

The result of the batch processing itself is defined in the batch
e.g., standard output, standard error output after the job, save
output file to external file systems.

Also, if you use the ``run`` subcommand, only standard output is
displayed. Standard error output cannot be printed to the same
console. In addition, to list the files in the sandbox you may use
``list-files``, and ``get-file`` to download files. As there are no
'download all files' interface, users must download each file
respectively, from a list obtained via ``list-files``.


Fault Tolerance
~~~~~~~~~~~~~~~

To make Retz queue fault torelant, first of all backend database must
configured fault tolerant - use H2 with persistent file, or
PostgreSQL. Retz has job recovery system inside, after restarting
process. Retz checks database at startup and if any running job found,
it checks Mesos to update latest status of those jobs.

High Availability
~~~~~~~~~~~~~~~~~

Currently Retz does not have any clustering feature such as clustering
or automatic fail over. Moreover, Retz has some important persistent
states like user info, application that'd be preferred to be saved
accross fail over.

Utilizing Marathon for automatic failover may work, if such persistent
data recovery into Retz process, in a new agent sandbox works. Also,
service discovery update accross failover may need setup like
[Mesos-DNS or Mesos-lb]
(https://mesosphere.github.io/marathon/docs/service-discovery-load-balancing.html)
.  It is necessary to construct a mechanism of service discovery such
as [Reference]
(https://open.mesosphere.com/tutorials/service-discovery/).



Retz administration tool
------------------------

``retz-admin`` supports creating, listing, disabling users as well as
usage statistics of each user, to monitor or to charge users.


Optimization for NUMA-aware program
-----------------------------------

When running NUMA aware programs such as Asakusa on M3BP on Mesos,
there may be a room for optimization by pinning threads to CPU cores
reather than time slices which cgroups system assumes.

Mesos is not aware of descrete CPU cores and some people in the
community feels like NUMA awareness, and JIRA issues below are tracker
for that. But it is not main direction of Mesos development process
for now, as those issues are not so active:

- `MESOS-6548 Support NUMA for tasks <https://issues.apache.org/jira/browse/MESOS-6548>`_
- `MESOS-5358 Design Doc for CPU pinning/binding support (MESOS-5342) <https://issues.apache.org/jira/browse/MESOS-5358>`_
- `MESOS-5342 CPU pinning/binding support for CgroupsCpushareIsolatorProcess <https://issues.apache.org/jira/browse/MESOS-5342>`_
- `MESOS-314 Support the cgroups 'cpusets' subsystem. <https://issues.apache.org/jira/browse/MESOS-314>`_


Monitoring Retz
---------------

Retz now has HTTP(S) endpoint for health check: ``/ping`` and
``/status``. ``/ping`` always returns ``OK`` in HTTP body. ``/status``
responds various metrics in JSON. Also, endpoing ``/`` is a good page
for humans, as it returns simple HTML web page for browsers, showing
part of server health status.

Retz outputs no logs other than standard output and standard error
output. Logs may be rotated or removed by daemonization tools. For
sustainable operation, using ``nohup`` is not recommended as it does
not handls output files well.
