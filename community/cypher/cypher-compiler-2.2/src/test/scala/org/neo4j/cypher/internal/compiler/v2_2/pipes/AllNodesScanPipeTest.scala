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
import org.neo4j.cypher.internal.helpers.CoreMocker

class AllNodesScanPipeTest extends CypherFunSuite with CoreMocker {

  private implicit val monitor = mock[PipeMonitor]

  test("should scan all nodes") {
    // given
    val mockedQueryContext = mockedGraphWithNodes(0, 1)
    val queryState = QueryStateHelper.emptyWith(
      query = mockedQueryContext
    )

    // when
    val result = AllNodesScanPipe("a", RowSpec(nodes = Seq("a")), 0)().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(mockedQueryContext.nodeOps.all.toList)
  }
}
