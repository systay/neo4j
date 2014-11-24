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

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution._
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.graphdb.{Relationship, Node}

import scala.collection.immutable.HashMap
import scala.collection.{Set, mutable}

class SpecedExecutionContext(nodes: Array[Long], rels: Array[Long], other: Array[AnyRef], spec: RowSpec, query: QueryContext) extends ExecutionContext {

  def this(spec: RowSpec, query: QueryContext) =
    this(
      Array.fill[Long](spec.nodes.size)(-1),
      Array.fill[Long](spec.relationships.size)(-1),
      Array.fill[AnyRef](spec.other.size)(null),
      spec,
      query)


  def filter(f: ((String, Any)) => Boolean): mutable.Map[String, Any] = ???

  def toMap: Map[String, Any] = Map(spec.elements.map {
    case (k, NodeIndex(i))  => k -> query.nodeOps.getById(nodes(i))
    case (k, RelIndex(i))   => k -> query.relationshipOps.getById(rels(i))
    case (k, OtherIndex(i)) => k -> other(i)
  }: _*)

  def -=(key: String): ExecutionContext = ???

  def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = ???

  def newWith(newEntries: Seq[(String, Any)]): ExecutionContext = ???

  def newWith(newEntries: collection.Map[String, Any]): ExecutionContext = ???

  def newWith(newEntry: (String, Any)): ExecutionContext = ???

  def newFromMutableMap(newEntries: mutable.Map[String, Any]): ExecutionContext = ???

  def update(k: String, v: Any): Unit = ???

  protected def createWithNewMap(newMap: mutable.Map[String, Any]): ExecutionContext = ???

  def collect[B](pf: PartialFunction[(String, Any), B]): Seq[B] = ???

  def get(key: String): Option[Any] = spec.indexTo(key) map {
    case NodeIndex(idx)  => nullOr(nodes(idx), query.nodeOps.getById)
    case RelIndex(idx)   => nullOr(rels(idx), query.relationshipOps.getById)
    case OtherIndex(idx) => other(idx)
  }

  private def nullOr[T >: Null <: AnyRef](idx: Long, f: Long => T): T = if (idx == -1) null else f(idx)

  def getOrElse(k: String, default: => Any): Any =
    get(k).getOrElse(default)

  def newWith1(key1: String, value1: Any): ExecutionContext = ???

  def size: Int = ???

  def copy(m: mutable.Map[String, Any]): ExecutionContext = ???

  def copy(): ExecutionContext = new SpecedExecutionContext(nodes.clone(), rels.clone(), other.clone(), spec, query)

  def remove(key: String): Option[Any] = ???

  def +=(kv: (String, Any)): ExecutionContext = ???

  def apply(k: String): Any = (spec.indexTo(k) map {
    case NodeIndex(idx)  => query.nodeOps.getById(nodes(idx))
    case RelIndex(idx)   => query.relationshipOps.getById(nodes(idx))
    case OtherIndex(idx) => query.relationshipOps.getById(nodes(idx))
  }).orNull

  def contains(k: String): Boolean = ???

  def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = ???

  def iterator: Iterator[(String, Any)] = ???

  def ++(other: ExecutionContext): ExecutionContext = ???

  def foreach[U](f: ((String, Any)) => U): Unit = ???

  def keySet: Set[String] = ???

  def setNode(idx: Int, node: Node): ExecutionContext = {
    nodes(idx) = node.getId
    this
  }

  def getNode(idx: Int): Node = query.nodeOps.getById(nodes(idx))

  def setRelationship(idx: Int, rel: Relationship): ExecutionContext = {
    rels(idx) = rel.getId
    this
  }

  def getRelationship(idx: Int): Relationship = query.relationshipOps.getById(rels(idx))

  def setAnyRef(idx: Int, value: AnyRef): ExecutionContext = {
    other(idx) = value
    this
  }

  def getAnyRef(idx: Int): AnyRef = other(idx)
}
