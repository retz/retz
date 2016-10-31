Getting Started With Retz Server
================================

Retz needs JDK8 to build.

::

  $ git clone https://github.com/retz/retz
  $ cd retz
  $ make deb

or run `make rpm` for RPM-managed environment like Red Hat, Fedora,
CentOS Linux. It is also possible to run `make server-jar` to obtain
a jar file with all dependencies bundled.

Retz requires Mesos ( >= 1.0) running (see
[staring Mesos](https://mesos.apache.org/gettingstarted/) ),
optionally libnuma for its runtime. To check Mesos version,
run `mesos-master --version`.

Install the server:

::

  # sudo dpkg -i retz-server-x.y.z.deb


Create a `retz.properties` file according to your environment. The deb and rpm
packages install an `/opt/retz-server/etc/retz.properties` file with default
values, and this is also where the Retz server will look for that file if
not specified otherwise with the `-C` parameter.

The following options must be set in the `retz.properties` file:

* `retz.mesos = 192.168.100.128:5050` - A pair of IP address and port
  number where Mesos master is listening to. Thus Mesos master must be
  running
* `retz.bind = http://localhost:9090` - An URL of host name and port
  number where Retz will bind and start Web server (port number must
  be > 1024)
* `retz.access.key = deadbeef` - User key to access the Retz server
* `retz.access.secret = cafebabe` - Secret to access the Retz server

Other settings are optional and documented in the `default configuration file
<https://github.com/retz/retz/blob/master/retz-server/src/main/dist/etc/retz.properties>`_.

Retz is a program that runs just in foreground. To start Retz in
console, type

::

  # /opt/retz-server/bin/retz-server


And you can find Retz server started up by opening
`the web console <http://localhost:9090>`_ . If you want Retz server
daemonized, use external daemonization system like supervisord.

Or to run without installation, just run

::

  $ bin/retz-server

or

::

  # java -jar ./retz-server/build/libs/retz-server-x.y.z-all.jar -C retz.properties

for the jar version in your repository.

See getting-started-client.rst to see how to use this service with clients.