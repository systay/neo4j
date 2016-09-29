/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.ruel

import org.neo4j.cypher.internal.compiler.v3_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_1.ast.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_1.logical.plans.{Projection, SingleRow}
import org.neo4j.cypher.internal.ir.v3_1.{IdName, QueryGraph, QueryShuffle, RegularPlannerQuery, RegularQueryProjection, UnionQuery}

class RuelPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val planner = new RuelPlanner()

  test("simple return is planned") {

    val logicalPlanningContext = newMockedLogicalPlanningContext(mock[PlanContext])

    val expressions = Map("x" -> SignedDecimalIntegerLiteral("1")(null))
    val projection = RegularQueryProjection(expressions, QueryShuffle.empty)
    val queryGraph = QueryGraph.empty
    val plannerQuery = RegularPlannerQuery(queryGraph = queryGraph, projection, None)
    val query = UnionQuery(Seq(plannerQuery), distinct = false, returns = Seq(IdName("x")), None)

    val plan = planner.planQuery(query)(logicalPlanningContext)
    plan should equal(Projection(SingleRow()(solved), expressions)(solved))
  }
}
