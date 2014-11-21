/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_2.commands

import org.neo4j.cypher.internal.compiler.v2_2.{InternalException, ExecutionContext}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.MutableMaps
import org.neo4j.graphdb.{Relationship, Node}

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.{Iterator, Set, immutable}

case class MapExecutionContext(m: MutableMap[String, Any] = MutableMaps.empty) extends ExecutionContext {
  def filter(f: ((String,Any)) => Boolean): MutableMap[String, Any] = m.filter(f)

  def get(key: String): Option[Any] = m.get(key)

  def iterator: Iterator[(String, Any)] = m.iterator

  def size: Int = m.size

  def ++(other: ExecutionContext): ExecutionContext = copy(m = m ++ other.toMap)

  def foreach[U](f: ((String, Any)) => U) {
    m.foreach(f)
  }

  def +=(kv: (String, Any)): ExecutionContext = {
    m += kv
    this
  }

  def -=(key: String): ExecutionContext = {
    m -= key
    this
  }

  def remove(key: String): Option[Any] = {
    val r = m.get(key)
    this -= key
    r
  }

  def contains(k: String): Boolean = m.contains(k)

  def collect[B](pf: PartialFunction[(String, Any), B]): Seq[B] = m.collect(pf).toSeq

  def keySet: Set[String] = m.keySet

  def getOrElse(k: String, default: => Any): Any = m.getOrElse(k, default)

  def update(k: String, v: Any) {
    m(k) = v
  }

  def apply(k: String): Any = m(k)

  def toMap: Map[String, Any] = m.toMap

  def newWith(newEntries: Seq[(String, Any)]): ExecutionContext =
    createWithNewMap(m.clone() ++= newEntries)

  def newWith(newEntries: scala.collection.Map[String, Any]): ExecutionContext =
    createWithNewMap(m.clone() ++= newEntries)

  def newFromMutableMap(newEntries: scala.collection.mutable.Map[String, Any]): ExecutionContext =
    createWithNewMap(newEntries)

  def newWith(newEntry: (String, Any)): ExecutionContext =
    createWithNewMap(m.clone() += newEntry)

  // This may seem silly but it has measurable impact in tight loops

  def newWith1(key1: String, value1: Any): ExecutionContext = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    createWithNewMap(newMap)
  }

  def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    createWithNewMap(newMap)
  }

  def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    newMap.put(key3, value3)
    createWithNewMap(newMap)
  }

  override def clone(): ExecutionContext = createWithNewMap(m.clone())

  protected def createWithNewMap(newMap: MutableMap[String, Any]): ExecutionContext = {
    copy(m = newMap)
  }

  override def toString(): String = {
    val contents = m.map { case (k,v) => s"$k -> $v" }.mkString(", ")
    s"ExecutionContext($contents)"
  }

  def copy(m: MutableMap[String, Any]): ExecutionContext = MapExecutionContext(m.clone())

  def copy(): ExecutionContext = copy(m.clone())

  def setNode(idx: Int, node: Node): ExecutionContext = !!!
  def getNode(idx: Int): Node = !!!
  def setRelationship(idx: Int, rel: Relationship): ExecutionContext = !!!
  def getRelationship(idx: Int): Relationship = !!!

  def !!! : Nothing = throw new InternalException("Ronja plans should not use MapExecutionContext")
}
