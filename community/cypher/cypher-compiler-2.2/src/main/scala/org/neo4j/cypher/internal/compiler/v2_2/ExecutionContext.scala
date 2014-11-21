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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.commands.MapExecutionContext
import org.neo4j.graphdb.{Relationship, Node}

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.{Iterator, Set, immutable, mutable}

object ExecutionContext {
  def empty: ExecutionContext = new MapExecutionContext()
  def apply(x: (String, Any)*): ExecutionContext = new MapExecutionContext().newWith(x)
  def apply(m: MutableMap[String, Any]): ExecutionContext = new MapExecutionContext(m)
}

trait ExecutionContext extends (String => Any) {
  def filter(f: ((String,Any)) => Boolean): MutableMap[String, Any]
  def get(key: String): Option[Any]
  def iterator: Iterator[(String, Any)]
  def size: Int
  def ++(other: ExecutionContext): ExecutionContext
  def foreach[U](f: ((String, Any)) => U)
  def +=(kv: (String, Any)): ExecutionContext
  def -=(key: String): ExecutionContext
  def remove(key: String): Option[Any]
  def contains(k: String): Boolean
  def collect[B](pf: PartialFunction[(String, Any), B]): Seq[B]
  def keySet: Set[String]
  def getOrElse(k: String, default: => Any): Any
  def update(k: String, v: Any)
  def apply(k: String): Any
  def toMap: immutable.Map[String, Any]
  def newWith(newEntries: Seq[(String, Any)]): ExecutionContext
  def newWith(newEntries: scala.collection.Map[String, Any]): ExecutionContext
  def newFromMutableMap(newEntries: scala.collection.mutable.Map[String, Any]): ExecutionContext
  def newWith(newEntry: (String, Any)): ExecutionContext
  def newWith1(key1: String, value1: Any): ExecutionContext
  def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext
  def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext
  protected def createWithNewMap(newMap: MutableMap[String, Any]): ExecutionContext
  def copy(m: mutable.Map[String, Any]): ExecutionContext
  def copy(): ExecutionContext

  // New SpecExeContext methods
  def setNode(idx: Int, id: Node): ExecutionContext
  def getNode(idx: Int): Node
  def setRelationship(idx: Int, rel: Relationship): ExecutionContext
  def getRelationship(idx: Int): Relationship
}

