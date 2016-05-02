# YAESS 2

This provides single fat jar executor to run AsakusaFW with several options:

* Choose execution engine from M3BP, single-node runner and Spark
* Choose execution environment from
  * Vanilla operating system
  * Mesos-managed cluster as oneshot framework
  * Mesos-managed cluster, as a simple job queue
  * Mesos-managed cluster, as a long running framework

## Getting Started

```sh
$ git clone git@bitbucket.org/asakusafw/yaess2
$ cd yaess2
$ ./gradlew shadowJar
$ ASAKUSA_HOME=/where/you/have/asakusa/application java -jar ./build/libs/yaess2-all.jar
```

## Related Projects
* [Asakusa Framework](https://github.com/asakusafw/asakusafw)
* [Asakusa Framework Examples](https://github.com/asakusafw/asakusafw-examples)
* [Asakusa Framework Legacy Modules](https://github.com/asakusafw/asakusafw-legacy)
* [Jinrikisha](https://github.com/asakusafw/asakusafw-starter)
* [Shafu](https://github.com/asakusafw/asakusafw-shafu)

## Resources
* [Asakusa Framework Documentation (ja)](http://docs.asakusafw.com/)
* [Asakusa Framework Community Site (ja)](http://asakusafw.com)

## Bug reports, Patch contribution
* Please report any issues to [repository for issue tracking](https://github.com/asakusafw/asakusafw-issues/issues)
* Please contribute with patches according to our [contribution guide (Japanese only, English version to be added)](http://docs.asakusafw.com/latest/release/ja/html/contribution.html)

## License
* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
