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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, NotEquals}
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.ast.NotEquals
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.ast.NotEquals
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.neo4j.cypher.internal.compiler.v2_1.LabelId

class NodeHashJoinPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should build plans containing joins") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan                    => 200
      case Expand(_, IdName("b"), _, _, _, _) => 10000
      case _: Expand                          => 10
      case _: NodeHashJoin                    => 20
      case _                                  => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    produceLogicalPlan("MATCH (a)<-[r1]-(b)-[r2]->(c) RETURN b") should equal(
      Projection(
        Selection(
          Seq(NotEquals(Identifier("r1")_,Identifier("r2")_)_),
          NodeHashJoin("b",
            Expand( AllNodesScan("a"), "a", Direction.INCOMING, Seq(), "b", "r1" ),
            Expand( AllNodesScan("c"), "c", Direction.INCOMING, Seq(), "b", "r2" )
          )
        ),
        expressions = Map("b" -> Identifier("b")_)
      )
    )
  }
}
