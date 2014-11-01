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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.SingleRowPlanDescription
import org.neo4j.cypher.internal.compiler.v2_2.symbols.{CypherType, SymbolTable}
import org.scalatest.mock.MockitoSugar

import scala.collection.Map

class FakePipe(val data: Iterator[Map[String, Any]], newIdentifiers: (String, CypherType)*) extends Pipe with MockitoSugar {

  val _cache: List[Map[String, Any]] = data.toList

  def this(data: Traversable[Map[String, Any]], identifiers: (String, CypherType)*) = this(data.toIterator, identifiers:_*)

  val symbols: SymbolTable = SymbolTable(newIdentifiers.toMap)

  def internalCreateResults(state: QueryState) = executionContexts.toIterator

  def planDescription = SingleRowPlanDescription(pipe = this, identifiers = Set.empty)

  def exists(pred: Pipe => Boolean) = ???

  val monitor: PipeMonitor = mock[PipeMonitor]

  def dup(sources: List[Pipe]): Pipe = ???

  def sources: Seq[Pipe] = ???

  override def localEffects = Effects.NONE

  def executionContexts: List[ExecutionContext] = _cache.map(m => ExecutionContext(collection.mutable.Map(m.toSeq: _*)))
}

class LazyPipe(data: Iterator[Map[String, Any]], identifiers: (String, CypherType)*) extends Pipe with MockitoSugar {
  val symbols: SymbolTable = SymbolTable(identifiers.toMap)

  def internalCreateResults(state: QueryState) = data.map(m => ExecutionContext.apply(m.toSeq: _*))

  def planDescription = SingleRowPlanDescription(this, identifiers = identifiers.map(_._1).toSet)

  def exists(pred: Pipe => Boolean) = ???

  val monitor: PipeMonitor = mock[PipeMonitor]

  def dup(sources: List[Pipe]): Pipe = ???

  def sources: Seq[Pipe] = ???

  override def localEffects = Effects.NONE
}
