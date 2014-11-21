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
import org.neo4j.cypher.internal.compiler.v2_2.LabelId
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.RowSpec
import org.neo4j.cypher.internal.compiler.v2_2.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.helpers.CoreMocker
import org.neo4j.graphdb.Node
import org.mockito.Mockito

class NodeByLabelScanPipeTest extends CypherFunSuite with CoreMocker {

  implicit val monitor = mock[PipeMonitor]
  import Mockito.when

  test("should scan labeled nodes") {
    val n0 = newMockedNode(0)
    val n1 = newMockedNode(1)
    // given
    val nodes = List(n0, n1)
    val context = mock[QueryContext]
    val nodeOps = mock[Operations[Node]]
    when(context.getNodesByLabel(12)).thenReturn(nodes.iterator)
    when(context.nodeOps).thenReturn(nodeOps)
    when(nodeOps.getById(0)).thenReturn(n0)
    when(nodeOps.getById(1)).thenReturn(n1)

    val queryState = QueryStateHelper.emptyWith(query = context)

    // when
    val result = NodeByLabelScanPipe("a", Right(LabelId(12)), RowSpec(nodes = Seq("a")), 0)().createResults(queryState).toList

    // then
    result.map(_("a")).toList should equal(nodes)
  }
}
