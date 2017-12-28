/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4.attribution

import scala.collection.mutable
import scala.reflect.ClassTag

class Attributes(idGen: IdGen, private val startingAttributes: Attribute[_]*) {

  private val attributes = mutable.ArrayBuffer[Attribute[_]](startingAttributes: _*)

  def copy(from: Id): IdGen = new IdGen {
    override def id(): Id = {
      val to = idGen.id()
      for (a <- attributes) {
        a.copy(from, to)
      }
      to
    }
  }

  def copy(from: Id, to: Id): Unit = {
    for (a <- attributes) {
      a.copy(from, to)
    }
  }

  def addAttribute(a: Attribute[_]): Unit = {
    attributes.append(a)
  }

  def toString[T: ClassTag](tree: Any, f: T => Id): String = {
    import org.neo4j.cypher.internal.util.v3_4.Foldable._
    val objectMap = new mutable.HashMap[Int, String]()
    tree.findByAllClass[T].foreach {
      x =>
        val id = f(x)
        objectMap.put(id.x, x.toString)
    }

    val sb = new mutable.StringBuilder
    for (attr <- attributes) {
      sb ++= "\n"
      sb ++= attr.getClass.getSimpleName + "*"*120 +  "\n"
      val array = attr.array
      for {i: Int <- array.indices
           value = array(i)
           if value.hasValue
           obj = objectMap(i)
      } {
        sb ++= s"$obj : ${array(i)}\n"
      }
    }
    sb.result()

  }
}

object Attributes {

}