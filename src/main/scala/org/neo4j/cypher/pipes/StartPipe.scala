/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.neo4j.cypher.SymbolTable
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import org.neo4j.cypher.commands.{RelationshipIdentifier, Identifier, NodeIdentifier}
import java.lang.String

abstract class StartPipe[T <: PropertyContainer](inner: Pipe, name: String, createSource: Map[String,Any] => Iterable[T]) extends Pipe {
  def this(inner: Pipe, name: String, sourceIterable: Iterable[T]) = this(inner, name, m => sourceIterable)

  def symbolType: Identifier

  val symbols: SymbolTable = inner.symbols.add(Seq(symbolType))

  def foreach[U](f: (Map[String, Any]) => U) {
    inner.foreach(innerMap => {
      createSource(innerMap).foreach((x) => {
        f(innerMap ++ Map(name -> x))
      })
    })
  }

  def visibleName:String
  override def executionPlan(): String = inner.executionPlan() + "\r\n" + visibleName + "(" + name + ")"
}

class NodeStartPipe(inner: Pipe, name: String, createSource: Map[String,Any] => Iterable[Node])
  extends StartPipe[Node](inner, name, createSource) {
  def symbolType: Identifier = NodeIdentifier(name)
  def visibleName: String = "Nodes"
}

class RelationshipStartPipe(inner: Pipe, name: String, createSource: Map[String,Any] => Iterable[Relationship])
  extends StartPipe[Relationship](inner, name, createSource) {
  def symbolType: Identifier = RelationshipIdentifier(name)
  def visibleName: String = "Rels"
}