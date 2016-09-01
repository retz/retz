# Retz release notes

## 0.0.18

* Retry of compromised 0.0.17 release

## 0.0.17 (not release and no tag)

* Migrate most of RPCs on WebSocket to plain HTTP request-response style

## 0.0.16

* This is another release for debug build: log output of server and client
  are both in debug level.
* Fix a bug that environment variables on 'run' and 'schedule' which
  includes '=' in values were ignored. This bug has been resident since
  0.0.13.
* By cleaning up messaging from executor to scheduler, several messages
  from executors does not generate unnecessary exception any more

## 0.0.15

* This is a release for debug build: log output of server and client are
  both in debug level.
* Several debug log around WebSocket PING/PONG is added
* Add 'help' subcommand to client CLI

## 0.0.14

* Fix several stacktrace printing from stderr to log stream
* Change state representation at 'list' command
* Fix log output at assigned resouces on an offer

## 0.0.13 (not released and no tag)

* Introduce [Wercker](https://app.wercker.com/retz/retz/) as external
  CI service to keep sanity
* Fix exception in server side when Mesos sends message to scheduler
* Fix ordering of retz-client subcommand: '-C path/to/retz.properties'
  **must** be before subcommand, otherwise it fails, by moving to
  JCommander in client

## 0.0.12

* Improve output format of the result of 'list', aligning and duration
* Fix return value of 'get-job' success

## 0.0.11

* Change output format of the result of 'list' and sort it by Job id
* Fix bug keepalive interval being longer than Jetty idle timeout
* Support single range format of just a number like "-4", "-32" for
  CLI options like'-cpu', '-mem', '-disk', and '-gpu'

## 0.0.10

* Add job history compaction when it exceeds max payload size in client
* Add 'get-job' subcommand to CLI
* Add running and finished jobs to the result of 'list' subcommand
* Add GPU support

## 0.0.9

* Squash all past commits to remove confidential information

## 0.0.8

* Add retz.mesos.offer_interval as retz.config parameter to control
  resource offer interval
* Update to Mesos 1.0.0
* Show standard error/ouput regardless of job status

## 0.0.7

* Fix race condition on `run` subcommand in client side
* Add Apache License 2.0 header to all files
* Show standard error to console on `run`
* Remove log on every resource offer, but only at accepted time
* Add `-trustpvfiles` option to Client CLI to omit decompression cost

## 0.0.6

* Add persistent volumes support with `-P` options on `load-app`
  with preservation of extracted tarballs there
* Directly show stdout result by specifying `-R -`
* Add preset environment variables `RETZ_CPU`, `RETZ_MEM` and
  `RETZ_PVNAME` at executor on spawning application command
* Fix CLI block forever when an command execution fails
* Update Gradle version to 2.14.1
* Add basic test code to integration test with real Mesos system
  in docker image
