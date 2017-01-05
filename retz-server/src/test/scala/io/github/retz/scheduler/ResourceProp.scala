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
package io.github.retz.scheduler

import java.util.Properties

import io.github.retz.protocol.data.{Job, ResourceQuantity}
import org.junit.Test
import org.scalacheck.{Gen, Prop}
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers

// A property to correctly decode mesos.Proto.Resource to retz.mesos.Resource
class ResourceProp extends JUnitSuite {
  // val nat = Gen.oneOf(Gen.posInt, 0) // This does not somehow work
  val nat = for {i <- Gen.chooseNum(0, 65536)} yield i.toInt
  // val pos = Gen.posInt // This does not work somehow too
  val pos = for {i <- Gen.chooseNum(1, 65536)} yield i.toInt

  @Test
  def decodeResource(): Unit = {
    Checkers.check(Prop.forAll(pos, pos, nat, nat) {
      (cpus: Int, mem: Int, disk: Int, gpu: Int) => {
        println(cpus, mem, disk, gpu)

        val mesosResources = ResourceConstructor.construct(cpus, mem, disk, gpu)
        val retzResource: Resource = ResourceConstructor.decode(mesosResources)
        println(mesosResources)
        retzResource.cpu() == cpus && retzResource.cpu() > 0 &&
          retzResource.memMB() == mem && retzResource.memMB() > 0 &&
          retzResource.diskMB() == disk &&
          retzResource.gpu == gpu
      }
    })
  }

  val resourceQuantity: Gen[ResourceQuantity] =
    for {cpus <- Gen.posNum[Int]
         mem <- Gen.posNum[Int]
         gpus <- Gen.posNum[Int]
         ports <- Gen.posNum[Int]
         disk <- Gen.posNum[Int]}
      yield new ResourceQuantity(cpus, mem, gpus, ports, disk, 0)

  @Test
  def resourceQuantityProp(): Unit = {
    Checkers.check(Prop.forAll(Gen.posNum[Int], Gen.posNum[Int], Gen.posNum[Int], Gen.posNum[Int], resourceQuantity) {
      (cpus: Int, mem: Int, gpus: Int, ports: Int, q: ResourceQuantity) => {
        val job = new Job("x", "a", new Properties(), cpus+1, mem + 32, gpus, ports)
        val fit = q.fits(job)
        val nofit = cpus+1> q.getCpu || mem + 32 > q.getMemMB || gpus > q.getGpu || ports > q.getPorts || 0 > q.getDiskMB
        println(cpus+1, mem+32, gpus, ports, q, fit, nofit, fit != nofit)
        fit != nofit
      }
    })
  }
}