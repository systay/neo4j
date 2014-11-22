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
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.RowSpec
import org.neo4j.cypher.internal.helpers.CoreMocker
import org.neo4j.graphdb.Node

class NodeByIdSeekPipeTest extends CypherFunSuite with CoreMocker {

  implicit val monitor = mock[PipeMonitor]

  test("should seek node by id") {
    val id = 17
    val context = mockedGraphWithNodes(id)
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = context
    )

    // when
    val result = NodeByIdSeekPipe("a", EntityByIdExprs(Seq(Literal(id))), RowSpec(nodes = Seq("a")), 0)().createResults(queryState)

    // then
    result.map(_("a").asInstanceOf[Node].getId).toList should equal(List(id))
  }

  test("should seek nodes by multiple ids") {
    // given
    val nodeIds = List(42, 21, 11)
    val queryState = QueryStateHelper.emptyWith(
      query = mockedGraphWithNodes(nodeIds:_*)
    )

    // whens
    val result = NodeByIdSeekPipe("a", EntityByIdExprs(Seq(Literal(42), Literal(21), Literal(11))), RowSpec(nodes = Seq("a")), 0)().createResults(queryState)

    // then
    result.map(_("a").asInstanceOf[Node].getId).toList should equal(nodeIds)
  }
}
