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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.graphdb.Direction

class RowSpecTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should handle pattern nodes in row spec") {
    val nodes = Set(IdName("n"), IdName("m"))
    val plan = newMockedLogicalPlanWithQueryGraph(
      ids = nodes,
      solved = QueryGraph(patternNodes = nodes)
    )

    RowSpec.from(plan) should equal(RowSpec(nodes = Set("n", "m"), relationships = Set.empty, other = Set.empty))
  }

  test("should handle relationship patterns in row spec") {
    val r1 = IdName("r1")
    val r2 = IdName("r2")
    val relationships = Set(r1, r2)
    val plan = newMockedLogicalPlanWithQueryGraph(
      ids = relationships,
      solved = QueryGraph(patternRelationships = Set(
        PatternRelationship(r1, (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength),
        PatternRelationship(r2, (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
      )))

    RowSpec.from(plan) should equal(RowSpec(nodes = Set.empty, relationships = Set("r1", "r2"), other = Set.empty))
  }
}
