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
/**
  * Retz
  * Copyright (C) 2016 Nautilus Technologies, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package io.github.retz.db

import java.util.{Optional, Properties}

import io.github.retz.cli.TimestampHelper
import io.github.retz.protocol.data.{Application, Job, MesosContainer, User}
import io.github.retz.protocol.RetzDataGen
import io.github.retz.RetzGen
import org.junit.Test
import org.scalacheck.Prop._
import org.scalacheck.commands.Commands
import org.scalacheck.{Gen, Prop}
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

object DatabaseSpec extends Commands {

  case class State(users: Map[String, User],
                   applications: Map[String, Application],
                   queued: Map[Int, Job],
                   running: Map[Int, Job],
                   name: String) {
    def start(id : Int, taskId: String) : State = {
      var job: Job = queued.get(id).get
      //job.killed(TimestampHelper.now(), Optional.empty(), "reason")
      job.started(taskId, Optional.empty(), TimestampHelper.now())
      State(users, applications, queued - id, running + (id -> job), name)
    }
    def addUser(user: User) : State = {
      State(users + (user.keyId() -> user), applications, queued, running, name)
    }

    /*
  def findFit(cpu: Int, mem: Int, gpu: Int): Fit = {
    val q = queued.values.toSeq.sortWith((l,r) => l.scheduled() < r.scheduled())
    var fit = q.toList
        var totalCpu: Int = 0
        var totalMem: Int = 0
        while (res.next && totalCpu <= cpu && totalMem <= memMB) {
          val json: String = res.getString("json")
          val job: Job = MAPPER.readValue(json, classOf[Job])
          if (job == null) throw new AssertionError("Cannot be null!!")
          else if (totalCpu + job.cpu <= cpu && totalMem + job.memMB <= memMB) {
            ret.addOffer(job)
            totalCpu += job.cpu
            totalMem += job.memMB
          }
          else break //todo: break is not supported
        }
      finally if (res != null) res.close()
        Fit(cpu, mem, gpu, fit)
    }
    */

  }

  type Sut = Database

  override def canCreateNewSut(newState: State, initStates: Traversable[State], runningSuts: Traversable[Sut]): Boolean = {
    println("can we create new databaas?")
    initStates.forall(_.name != newState.name)
  }

  override def initialPreCondition(state: State): Boolean = {
    state.users.isEmpty && state.applications.isEmpty && state.queued.isEmpty
  }

  override def newSut(state: State): Sut = {
    println("starting")
    Database.newMemInstance(state.name)
  }

  override def destroySut(sut: Sut): Unit = {
    sut.clear()
    sut.stop()
    println("Stopped " + sut)
  }

  override def genInitialState: Gen[State] = for {
    name <- RetzGen.nonEmpty(32)
  } yield State(Map.empty, Map.empty, Map.empty, Map.empty, name)

  // Commands: schedule, scheduled, started, resource offer, job finished, job lost
  // Add user, addOffer application
  // Noop for validation...
  override def genCommand(state: State): Gen[Command] =
  if (state.users.isEmpty) {
    Gen.oneOf(Gen.const(Noop), genAddUser)
  } else if (state.applications.isEmpty) {
    Gen.oneOf(Gen.const(Noop), genAddUser, genAddApplication(state))
  //} else if (state.queued.isEmpty) {
  //  Gen.oneOf(Gen.const(Noop), genAddUser, genAddApplication(state), genSchedule(state))
  } else {
    Gen.oneOf(Gen.const(Noop), genAddUser, genAddApplication(state), genSchedule(state))//, genResourceOffer(state))
  }

  // For ScalaCheck sanity testing
  case object Noop extends UnitCommand {
    override def preCondition(state: State): Boolean = true

    override def run(sut: Database): Unit = ()

    override def nextState(state: State): State = state

    override def postCondition(state: State, success: Boolean): Prop = success
  }

  val genAddUser: Gen[AddUser] = for {
    user <- RetzDataGen.user
  } yield AddUser(user)

  case class AddUser(user: User) extends UnitCommand {
    override def preCondition(state: State): Boolean = true

    override def run(sut: Sut): Unit = {
      // As QuickCheck generators aren't quite good at generating unique set of names,
      // just doing kinda 'maybe' here; otherwise sut.addUser will throw an exception
      if (!sut.getUser(user.keyId()).isPresent) {
        println("Adding user " + user.keyId() + " to database! (" + sut.getUser(user.keyId()))
        sut.addUser(user)
      }
    }

    override def postCondition(state: State, success: Boolean) = success

    override def nextState(state: State): State =
      if (!state.users.contains(user.keyId())) {
        state.addUser(user)
      } else {
        state
      }
  }

  def genAddApplication(state: State): Gen[AddApplication] = for {
    owner <- Gen.oneOf(state.users.keys.toSeq)
    app <- RetzDataGen.application(owner)
  } yield AddApplication(app)

  case class AddApplication(application: Application) extends UnitCommand {
    override def preCondition(state: State): Boolean = true

    override def run(sut: Sut): Unit = {
      println("Adding application as overwrite: " + application.getAppid)
      sut.addApplication(application)
    }

    override def nextState(state: State): State = {
      val apps = state.applications - application.getAppid + (application.getAppid -> application)
      State(state.users, apps, state.queued, state.running, state.name)
    }

    override def postCondition(state: State, success: Boolean): Prop = success
  }

  def genSchedule(state: State): Gen[Schedule] = for {
    appid <- Gen.oneOf(state.applications.keys.toSeq)
    job <- RetzDataGen.job(appid)
    id <- Gen.posNum[Int]
  } yield {
    job.schedule(id, TimestampHelper.now())
    Schedule(id, job)
  }

  case class Schedule(id: Int, job: Job) extends UnitCommand {
    override def preCondition(state: State): Boolean = true

    override def run(sut: Sut): Unit =
      if (!sut.getJob(id).isPresent) {
        println("Adding job " + job.toString)
        sut.safeAddJob(job)
      }

    override def postCondition(state: State, success: Boolean) = success

    override def nextState(state: State): State = {
      val q = if (state.queued.contains(id)) {
        state.queued
      } else {
        state.queued - id + (id -> job)
      }
      State(state.users, state.applications, q, state.running, state.name)
    }
  }

  /*
  def genResourceOffer(state: State): Gen[ResourceOffer] = for {
    cpu <- Gen.chooseNum[Int](1, 20)
    mem <- Gen.chooseNum[Int](32, 65536)
    gpu <- Gen.chooseNum[Int](0, 8)
  } yield ResourceOffer(state.findFit(cpu, mem, gpu))

  case class Fit(cpu: Int, mem: Int, gpu: Int, jobs: List[Job]) {}

  case class ResourceOffer(fit: Fit) extends  Command {
    type Result = List[Job]
    override def preCondition(state: State): Boolean = true

    override def run(sut: Database): List[Job] = {
      var list = sut.findFit(fit.cpu, fit.mem).asScala
      list.map({ job =>
        val taskId: String = "boom-" + job.appid() + "-" + job.id()
        sut.setJobStarting(job.id(), Optional.empty(), taskId)
        job
      }).toList
    }

    override def postCondition(state: State, result: Try[List[Job]]): Prop = {
      result.ensuring(_.isSuccess)
      result.ensuring(_.get.size == fit.jobs.size)
      true
    }

    override def nextState(state: State): State = state
  }
  */
}

class DatabaseTestSuite extends JUnitSuite with Checkers {
  @Test
  def propDatabase(): Unit = check(DatabaseSpec.property())
}
