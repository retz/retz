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
import io.github.retz.mesos.{ResourceConstructor, Resource => RetzResource}
import org.apache.mesos.Protos.{Resource => MesosResource}
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
        val retzResource: RetzResource = ResourceConstructor.decode(mesosResources)
        println(mesosResources)
        retzResource.cpu() == cpus && retzResource.cpu() > 0 &&
          retzResource.memMB() == mem && retzResource.memMB() > 0 &&
          retzResource.diskMB() == disk &&
          retzResource.gpu == gpu
      }
    })
  }
}