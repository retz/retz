Getting Started With Retz Server
================================

Retz needs JDK8 to build.

::

  $ git clone https://github.com/retz/retz
  $ cd retz
  $ make deb

or run `make rpm` for RPM-managed environment like Red Hat, Fedora,
CentOS Linux.

Retz requires Mesos ( >= 1.0) running (see
[staring Mesos](https://mesos.apache.org/gettingstarted/) ),
optionally libnuma for its runtime. To check Mesos version,
run `mesos-master --version`.

Install the server:

::

  # sudo dpkg -i retz-server-x.y.z.deb


Edit `/opt/retz-server/etc/retz.properties` according to your environment:

* `retz.mesos = 192.168.100.128:5050` - A pair of IP address and port
  number where Mesos master is listening to. Thus Mesos master must be
  running
* `retz.bind = http://localhost:9090` - An URL of host name and port
  number where Retz will bind and start Web server (port number must
  be > 1024)
* `retz.mesos.principal = retz` - Mesos principal name
* `retz.mesos.role = retz` - Role name in Mesos
* `retz.mesos.secret.file` - A file path containing mesos authentication secret (optional, no line breaks allowed in the file)

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


in your repository.

See getting-started-client.rst to see how to use this service with clients.