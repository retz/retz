/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.protocol

import java.util.{Optional, Properties}

import io.github.retz.protocol.data.{Application, Job, MesosContainer, User}
import org.scalacheck.Gen
import io.github.retz.RetzGen
import io.github.retz.cli.TimestampHelper
import io.github.retz.scheduler.RetzSchedulerTest
import org.apache.mesos.Protos.FrameworkID
import org.apache.mesos.Protos

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RetzDataGen {

  // Job
  // Mesos Resource

  def user: Gen[User] = for {
    key <- RetzGen.nonEmpty(32)
    secret <- RetzGen.nonEmpty(64)
    info <- Gen.alphaStr
  } yield new User(key, secret, true, info)

  def application(owner: String): Gen[Application] = for {
    appid <- RetzGen.nonEmpty(32)
    largeFiles <- Gen.containerOf[List, String](RetzGen.url)
    files <- Gen.containerOf[List, String](RetzGen.url)
    diskMB <- Gen.chooseNum(0, 10)
    gracePeriod <- Gen.chooseNum[Int](0, 1024)
    unixUser <- RetzGen.nonEmpty
  } yield new Application(appid, largeFiles.asJava, files.asJava,
    Optional.of(unixUser), owner, gracePeriod, new MesosContainer(), true)

  def job(appid: String) : Gen[Job] = for {
    name <- RetzGen.nonEmpty
    cmd <- RetzGen.nonEmpty
    id <- Gen.posNum[Int]
  } yield {
    var job = new Job(appid, cmd, new Properties(), 1, 32, 1, 0)
    job.schedule(id, TimestampHelper.now())
    job
  }

  // val nat = Gen.oneOf(Gen.posInt, 0) // This does not somehow work
  val nat = for {i <- Gen.chooseNum(0, 65536)} yield i.toInt
  // val pos = Gen.posInt // This does not work somehow too
  val pos = for {i <- Gen.chooseNum(1, 65536)} yield i.toInt

  def offer(frameworkID: String) : Gen[Protos.Offer] = for {
    cpus <- nat
    mem <- Gen.chooseNum(32, 512*1024)
    uuid <- RetzGen.nonEmpty
    slaveId <- RetzGen.nonEmpty
  } yield {
    RetzSchedulerTest.buildOffer(frameworkID, slaveId, uuid, cpus, mem)
  }
}
