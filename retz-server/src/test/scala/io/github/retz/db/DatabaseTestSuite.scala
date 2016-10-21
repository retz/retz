/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
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
package io.github.retz.db

import io.github.retz.protocol.data.User
import org.junit.Test
import org.scalacheck.Prop._
import org.scalacheck.commands.Commands
import org.scalacheck.{Gen, Prop}
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers

object DatabaseSpec extends Commands {

  case class State(users: Map[String, User],
                   name: String)

  type Sut = Database

  override def canCreateNewSut(newState: State, initStates: Traversable[State], runningSuts: Traversable[Sut]): Boolean = {
    //System.err.println("can we create new databaas?")
    initStates.forall(_.name != newState.name)
  }

  override def initialPreCondition(state: State): Boolean = {
    state.users.isEmpty
  }

  override def newSut(state: State): Sut = {
    println("starting")
    Database.newMemInstance(state.name)
  }

  override def destroySut(sut: Sut): Unit = {
    //println("Stopping")
    sut.clear()
    sut.stop()
    println("Stopped " + sut)
  }

  override def genInitialState: Gen[State] = for {
    name <- RetzGen.nonEmpty(32)
  } yield State(Map.empty, name)

  // Commands: schedule, scheduled, started, resource offer, job finished, job lost
  // Add user, add application
  // Noop for validation...
  override def genCommand(state: State): Gen[Command] =
  Gen.oneOf(Gen.const(Noop), genAddUser)

  /*
    if (state.users.isEmpty) {
      genAddUser(state)
    } else if(state.applications.isEmpty) {
      Gen.oneOf(genAddUser(state), genAddApplication(state))
    } else {
      Gen.oneOf(genAddUser(state), genAddApplication(state))
    } */

  case object Noop extends UnitCommand {
    override def preCondition(state: State): Boolean = true

    override def run(sut: Database): Unit = ()

    override def nextState(state: State): State = state

    override def postCondition(state: State, success: Boolean): Prop = success
  }

  val genAddUser: Gen[AddUser] = for {
    key <- RetzGen.nonEmpty(32)
    secret <- RetzGen.nonEmpty(64)
  } yield AddUser(new User(key, secret, true))

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
        State(state.users + (user.keyId() -> user), state.name)
      } else {
        state
      }
  }

  /*
    def genApplication(state: State): Gen[Application] = for {
      appid <- RetzGen.nonEmpty
      persistentFiles <- Gen.containerOf[List, String](RetzGen.url)
      largeFiles <- Gen.containerOf[List, String](RetzGen.url)
      files <- Gen.containerOf[List, String](RetzGen.url)
      diskMB <- Gen.chooseNum(0, 10)
      unixUser <- RetzGen.nonEmpty
      owner <- Gen.oneOf(state.users.keys.toSeq)
    } yield new Application(appid, persistentFiles.asJava, largeFiles.asJava, files.asJava,
      java.util.Optional.of(diskMB), Optional.of(unixUser), owner, new MesosContainer(), true)

    def genAddApplication(state: State): Gen[AddApplication] = for {
      application <- genApplication(state)
    } yield AddApplication(application)

    case class AddApplication(application: Application) extends UnitCommand {
      override def preCondition(state: State): Boolean = {
        //Database.getUser(application.getOwner).isPresent
        println("do we have user " + application.getOwner + "? -> " + (state.applications contains(application.getOwner)))
        state.applications contains(application.getOwner)
      }

      override def run(sut: Sut): Unit = {
        println("Adding application " + application.getAppid)
        sut.addApplication(application)
      }

      override def nextState(state: State): State = {
        state.applications(application.getAppid) = application
        state
      }

      override def postCondition(state: State, success: Boolean): Prop = success
    }
    def genJob(state: State): Gen[Job] = for {
      appid <- Gen.oneOf(state.applications.keys.toSeq)
      name <- RetzGen.nonEmpty
      cmd <- RetzGen.nonEmpty
    } yield new Job(appid, cmd, new Properties(), 1, 1, 1)

    def genSchedule(state: State): Gen[Schedule] = for {
      job <- genJob(state)
      id <- Gen.posNum[Int]
    } yield Schedule(id, job)

    case class Schedule(id: Int, job: Job) extends UnitCommand {
      override def preCondition(state: State): Boolean = {
        Database.getApplication(job.appid()).isPresent &&
          !Database.getJob(id).isPresent &&
          !(state.jobs contains id)
      }

      override def run(sut: Sut): Unit = {
        println("Adding job " + job.toString)
        job.schedule(id, TimestampHelper.now())
        Database.safeAddJob(job)
      }

      override def nextState(state: State): State = {
        state.jobs(job.id()) = job
        state
      }

      override def postCondition(state: State, success: Boolean): Prop = {
        val j = Database.getJob(id).get()
        j.id() == id
      }
    }
  */
}

class DatabaseTestSuite extends JUnitSuite with Checkers {
  @Test
  def propDatabase(): Unit = check(DatabaseSpec.property())
}
