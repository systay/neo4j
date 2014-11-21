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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.RowSpec
import org.neo4j.cypher.internal.compiler.v2_2.spi.{Operations, QueryContext}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.helpers.CoreMocker
import org.neo4j.graphdb.{Node, Direction, Relationship}
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.commands.{SpecedExecutionContext, Not, Predicate, True}

class ExpandPipeTest extends CypherFunSuite with CoreMocker {

  implicit val monitor = mock[PipeMonitor]
  val startNode = newMockedNode(1)
  val endNode1 = newMockedNode(2)
  val endNode2 = newMockedNode(3)
  val relationship1 = newMockedRelationship(1, startNode, endNode1)
  val relationship2 = newMockedRelationship(2, startNode, endNode2)
  val selfRelationship = newMockedRelationship(3, startNode, startNode)
  val query = mock[QueryContext]
  val nodeOps = mock[Operations[Node]]
  val relOps = mock[Operations[Relationship]]
  when(query.nodeOps).thenReturn(nodeOps)
  when(query.relationshipOps).thenReturn(relOps)
  when(nodeOps.getById(1)).thenReturn(startNode)
  when(nodeOps.getById(2)).thenReturn(endNode1)
  when(nodeOps.getById(3)).thenReturn(endNode2)
  when(relOps.getById(1)).thenReturn(relationship1)
  when(relOps.getById(2)).thenReturn(relationship2)
  when(relOps.getById(3)).thenReturn(selfRelationship)
  val queryState = QueryStateHelper.emptyWith(query = query)

  def createPipe(left: Pipe) =
    ExpandPipeForStringTypes(left, 0, "a", 0, 1, "r", "b", Direction.OUTGOING, Seq.empty)()

  test("should support expand between two nodes with a relationship") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row(startNode))

    // when
    val result = createPipe(left).createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    mockRelationships(relationship1, relationship2)
    val left = newMockedPipe("a",
      row(startNode))

    // when
    val result = createPipe(left).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
    second.toMap should equal(Map("a" -> startNode, "r" -> relationship2, "b" -> endNode2))
  }

  test("should support expand between two nodes with multiple relationships and self loops") {
    // given
    mockRelationships(relationship1, selfRelationship)
    val left = newMockedPipe("a",
      row(startNode))

    // when
    val result = createPipe(left).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
    second.toMap should equal(Map("a" -> startNode, "r" -> selfRelationship, "b" -> startNode))
  }

  test("given empty input, should return empty output") {
    // given
    mockRelationships()
    val left = newMockedPipe("a", row(null))

    // when
    val result = createPipe(left).createResults(queryState).toList

    // then
    result should be (empty)
  }

  test("given a null start point, returns an empty iterator") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row(startNode))

    // when
    val result = createPipe(left).createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
  }

  private def row(node: Node) = {
    val nodes = node match {
      case null    => Array(-1L, -1L)
      case n: Node => Array(n.getId, -1L)
    }
    new SpecedExecutionContext(
      nodes = nodes,
      rels = Array(-1L),
      other = Array.empty,
      spec = RowSpec(nodes = Seq("a", "b"), relationships = Seq("r")), query)
  }
  private def mockRelationships(rels: Relationship*) {
    when(query.getRelationshipsFor(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = rels.iterator
    })
  }

  private def newMockedPipe(node: String, rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(SymbolTable(Map(node -> CTNode)))
    when(pipe.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = rows.iterator
    })

    pipe
  }
}
