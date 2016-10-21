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

import org.scalacheck.Gen

object RetzGen {

  def nonEmpty(maxLen: Int): Gen[String] =
    for {
      len <- Gen.chooseNum(1, maxLen)
      list <- Gen.listOfN(len, Gen.alphaChar)
    } yield list.mkString

  def nonEmpty: Gen[String] =
    for {list <- Gen.nonEmptyListOf[Char](Gen.alphaChar)}
      yield list.mkString

  def host: Gen[String] = for {
    domains <- Gen.resize(2, Gen.nonEmptyContainerOf[List, String](nonEmpty))
  } yield domains.mkString(".")

  def maybePort: Gen[String] = for {
    port <- Gen.chooseNum(1, 65535)
  } yield if (port == 80) {
    ""
  }
  else {
    ":" + port.toString
  }

  def resource: Gen[String] = for {
    nodes <- Gen.resize(2, Gen.nonEmptyContainerOf[List, String](nonEmpty))
  } yield "/" + nodes.mkString("/")

  def url: Gen[String] = for {
    scheme <- Gen.oneOf("http", "https")
    domain <- host
    port <- maybePort
    resource <- resource
  } yield scheme + "://" + domain + port + resource
}