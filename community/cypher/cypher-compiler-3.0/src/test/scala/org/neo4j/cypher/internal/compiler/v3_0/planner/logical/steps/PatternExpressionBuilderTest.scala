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

import scala.collection.mutable

class PatternExpressionBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("Rewrites single pattern expression") {
    // given expression (a)-->(b)
    val b = newMockedLogicalPlan("b")
    val strategy = createStrategy(b)
    implicit val context = newMockedLogicalPlanningContext(newMockedPlanContext, strategy = strategy)
    val source = newMockedLogicalPlan("a")
    val pathStep = mock[PathStep]
    val mockPathStepBuilder: EveryPath => PathStep = _ => pathStep

    val step1 = patternExpressionBuilder(mockPathStepBuilder)
    // when
    val (resultPlan, expressions) = step1(source, Map("x" -> patExpr1))

    // then
    val expectedInnerPlan = Projection(b, Map("  FRESHID0" -> PathExpression(pathStep)(pos)))(solved)

    resultPlan should equal(RollUp(source, expectedInnerPlan, IdName("x"), IdName("  FRESHID0"), Set(IdName("a")))(solved))
    expressions should equal(Map("x" -> Variable("x")(pos)))
  }

  test("Rewrites multiple pattern expressions") {
    // given expression (a)-->(b)
    val b = newMockedLogicalPlan("b")
    val strategy = createStrategy(b)
    implicit val context = newMockedLogicalPlanningContext(newMockedPlanContext, strategy = strategy)
    val source = newMockedLogicalPlan("a")
    val pathStep1 = mock[PathStep]
    val pathStep2 = mock[PathStep]
    val stack = mutable.Stack[PathStep]()
    stack.push(pathStep1, pathStep2)

    val mockPathStepBuilder : EveryPath => PathStep = _ => stack.pop()

    // when
    val (resultPlan, expressions) = patternExpressionBuilder(mockPathStepBuilder)(source, Map("x" -> patExpr1, "y" -> patExpr2))

    // then
    val expectedInnerPlan1 = Projection(b, Map("  FRESHID0" -> namedPatExpr1))(solved)
    val rollUp1 = RollUp(source, expectedInnerPlan1, IdName("x"), IdName("  FRESHID0"), Set(IdName("a")))(solved)

    val expectedInnerPlan2 = Projection(b, Map("  FRESHID0" -> namedPatExpr2))(solved)
    val rollUp2 = RollUp(rollUp1, expectedInnerPlan2, IdName("y"), IdName("  FRESHID0"), Set(IdName("a")))(solved)


    resultPlan should equal()
    expressions should equal(Map("x" -> Variable("x")(pos), "y" -> Variable("y")(pos)))
  }

  private val patExpr1 = newPatExpr("a")
  private val patExpr2 = newPatExpr("a", SemanticDirection.INCOMING)
  private val namedPatExpr1 = newPatExpr("a", "  UNNAMED1")
  private val namedPatExpr2 = newPatExpr("a", "  UNNAMED1", SemanticDirection.INCOMING)

  private def newPatExpr(left: String, dir: SemanticDirection = SemanticDirection.OUTGOING): PatternExpression = newPatExpr(left, None, None, dir)
  private def newPatExpr(left: String, right: String): PatternExpression = newPatExpr(left, Some(right), None, SemanticDirection.INCOMING)
  private def newPatExpr(left: String, right: String, dir: SemanticDirection): PatternExpression = newPatExpr(left, Some(right), None, dir)

  private def newPatExpr(left: String, right: Option[String], relName: Option[String], dir: SemanticDirection): PatternExpression = {
    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor(left)), Seq.empty, None) _,
      RelationshipPattern(None, optional = false, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
      NodePattern(right.map(varFor), Seq.empty, None) _) _) _)
  }

  private def createStrategy(plan: LogicalPlan): QueryGraphSolver = {
    val strategy = mock[QueryGraphSolver]
    when(strategy.plan(any[QueryGraph])(any[LogicalPlanningContext], any())).thenReturn(plan)
    strategy
  }
}
