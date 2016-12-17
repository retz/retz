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
package io.github.retz.scheduler

import java.util.Optional
import org.junit.Test
import org.scalacheck.{Gen, Prop}
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers
import io.github.retz.protocol.data.{Application, Job}
import io.github.retz.protocol.RetzDataGen
import io.github.retz.RetzGen
import org.apache.mesos.Protos.{FrameworkID, Offer}

import scala.collection.JavaConverters._

class PlannerPropTest extends JUnitSuite {

  // val nat = Gen.oneOf(Gen.posInt, 0) // This does not somehow work
  val nat = for {i <- Gen.chooseNum(0, 65536)} yield i.toInt
  // val pos = Gen.posInt // This does not work somehow too
  val pos = for {i <- Gen.chooseNum(1, 65536)} yield i.toInt

  val jobs = for {
    len <- Gen.chooseNum(1, 64)
    owner <- RetzGen.nonEmpty
    appid <- RetzGen.nonEmpty
    jobs <- Gen.listOfN(len, RetzDataGen.job(appid))
  } yield jobs

  def offers(fid: String) = for {
    len <- Gen.chooseNum[Int](1, 64)
    offer <- RetzDataGen.offer(fid)
    offers <- Gen.listOfN(len, RetzDataGen.offer(fid))
  } yield (offer :: offers)

  // input: offers and jobs, with max stock
  // output:
  //private List<Protos.Offer.Operation> operations;
  //private List<Job> toBeLaunched;
  //private List<Job> toCancel;
  //private List<Protos.OfferID> toBeAccepted;
  //private List<Protos.OfferID> toDecline;
  //private List<Protos.Offer> toStock;

  // Invariants;
  //   (Offered resources) == (Accepted resources) + (Stocked Resources)
  //   (Job candidates) == (Job launched) + (Job cancelled)
  //   (Offer IDs) == (Offer IDs to decline) + (Offer IDs to accept)

  //Plan plan(List<Protos.Offer> offers, List<AppJobPair> jobs, int maxStock);
  var owner: String = "deadbeef"
  var fid : String = "framewark-id"
  @Test
  def plannerInvariantProp(): Unit = {
    Checkers.check(Prop.forAll(
      Gen.oneOf("naive", "priority"),
      RetzDataGen.application(owner),
      jobs,
      offers(fid),
      Gen.posNum[Int]) {
      (plannerName: String,
       application: Application,
       jobs: List[Job],
       offers: List[Offer],
       maxStock) => {
        var appJobs = jobs.map(job => new AppJobPair(Optional.of(application), job))
        var planner: Planner = PlannerFactory.create(plannerName);
        var plan: Plan = planner.plan(offers.asJava, appJobs.asJava, maxStock, "nobody");
        var totalJobsToLaunch = plan.getOfferAcceptors.asScala.foldLeft(0)( (sum, acceptor) => sum + acceptor.getJobs.size() )
        println(jobs.size, offers.size, maxStock, "=>", plan.getOfferAcceptors.size(), plan.getToStock.size())

        plan.getOfferAcceptors.asScala.foreach(acceptor => acceptor.verify());

        (plan.getOfferAcceptors.size() + plan.getToStock.size() == offers.size) &&
          (maxStock >= plan.getToStock.size()) &&
          (totalJobsToLaunch + plan.getToKeep.size() == jobs.size)
      }
    })
  }
}