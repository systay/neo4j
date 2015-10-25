/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class PatternExpressionBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("Rewrites single pattern expression") {
    // given expression (a)-->(b)
    val b = newMockedLogicalPlan("b")
    val strategy = createStrategy(b)
    implicit val context = newMockedLogicalPlanningContext(newMockedPlanContext, strategy = strategy)
    val source = newMockedLogicalPlan("a")

    // when
    val (resultPlan, expressions) = patternExpressionBuilder(source, Map("x" -> patExpr1))

    // then
    val expectedInnerPlan = Projection(b, Map("  FRESHID0" -> namedPatExpr1))(solved)

    resultPlan should equal(RollUp(source, expectedInnerPlan, IdName("x"), IdName("  FRESHID0"), Set(IdName("a")))(solved))
    //    expressions should equal(Map("  FRESHID0" -> Identifier("  FRESHID0")(pos)))
  }

  private val patExpr1 = newPatExpr("a", "b")
  private val namedPatExpr1 = newPatExpr("a", "b")
  private val patExpr2 = newPatExpr("c", "d")
  private val patExpr3 = newPatExpr("e", "f ")
  private val patExpr4 = newPatExpr("g", "h")

  private val dummyPlan = AllNodesScan(IdName("a"), Set.empty)(solved)

  private def newPatExpr(left: String, right: String, relName: String): PatternExpression = newPatExpr(left, right, Some(relName))
  private def newPatExpr(left: String, right: String, relName: Option[String] = None): PatternExpression = {
    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(ident(left)), Seq.empty, None) _,
      RelationshipPattern(None, optional = false, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
      NodePattern(Some(ident(right)), Seq.empty, None) _) _) _)
  }

  private def createStrategy(plan: LogicalPlan): QueryGraphSolver = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any[QueryGraph])(any[LogicalPlanningContext], any())).thenReturn(plan)
    strategy
  }
}
