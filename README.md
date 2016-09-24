# Retz [![wercker status](https://app.wercker.com/status/8aa503883d1a504ebd34ae72b9ac9dfd/s/master "wercker status")](https://app.wercker.com/project/byKey/8aa503883d1a504ebd34ae72b9ac9dfd)

Retz is a simple job queuing and execution service which runs on top
of [Mesos](https://mesos.apache.org) as a framework.

## Getting Started

Retz needs JDK8 to build.

```sh
$ git clone https://github.com/retz/retz
$ cd retz
$ make deb
```

or run `make rpm` for RPM-managed environment like Red Hat, Fedora,
CentOS Linux.

Retz requires Mesos ( >= 1.0) running (see
[staring Mesos](https://mesos.apache.org/gettingstarted/) ),
optionally libnuma for its runtime. To check Mesos version,
run `mesos-master --version`.

Install the server:

```
# sudo dpkg -i retz-server-x.y.z.deb
```

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

```
# /opt/retz-server/bin/retz-server
```

And you can find Retz server started up by opening
[the web console](http://localhost:9090). If you want Retz server
daemonized, use external daemonization system like supervisord.

Or to run without installation, just run

```
$ bin/retz-server
```

in your repository.

To run basic test, follow this with `/opt/retz-client/bin` in your
`$PATH` environment variable and edit
`/opt/retz-client/etc/retz.properties` as well as server. To test your
Retz setup, run

```
$ retz-client load-app -A test
$ retz-client run -A test -cmd 'uname -a'
```

and you'll have result of `uname -a` in some arbitrary Mesos Agent
node.

## Kick a job with resources allocated via Retz Mesos framework

Make your application loaded on Retz:

```
$ retz-client load-app -A your-app-name -F http://example.com/path/to/your/application.tar.gz
```

With `-F` option, any type of files is automatically downloaded by
mesos and the file is cached in the Agent node. In addition to `http`,
`hdfs` is also available. With subcommand `retz-client list-app`
you'll see your application is registered to Retz. To run a job via
Retz scheduler:

```
$ retz-client run -A your-app-name -cmd 'relative/path/to/your/bin/cmd -some-args' -R -
```

where `cmd` is included in your application tar ball. Then you'll get
results printed at standard output of your console. The result is also
available via Mesos sandbox UI. `run` is a synchronous execution
subcommand while `schedule` is an asynchronous execution subcommand.


## Documentation

See and walk around `doc` directory for documents.

## Related Projects
* [Asakusa Framework](https://github.com/asakusafw/asakusafw)
* [Asakusa on M3BP](https://github.com/asakusafw/asakusafw-m3bp)

## Reporting issues
* Please report any issues to [repository for issue tracking](https://github.com/retz/retz/issues)

## How to contribute
1. Clone this repository
1. Make a topic branch
1. If it is a bugfix, add test(s) to reproduce the bug
1. Add your your modification
1. Make sure nothing is broken by running `make build inttest`
1. Commit with proper description in the commit message
1. Open a pull request to [Retz repository](https://github.com/retz/retz)

* By opening a pull request, we assume the contributor has agreed to donate
  all copyright of the patch to the original author Nautilus Technologies
  and agreed not to claim any intellectual properties in the patch.

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
