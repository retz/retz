# Retz release notes

## 0.0.10

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
