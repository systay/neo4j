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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class PlanEagernessTest extends CypherFunSuite with LogicalPlanConstructionTestSupport {

  implicit var context: LogicalPlanningContext = null
  var lpp: LogicalPlanProducer = null

  override protected def initTest(): Unit = {
    super.initTest()
    context = mock[LogicalPlanningContext]
    lpp = mock[LogicalPlanProducer]
    when(context.logicalPlanProducer).thenReturn(lpp)
  }

  test("no eager is needed!") {
    val codeUndertest = PlanEagerness(FakePlanner(mock[LogicalPlan]))
    val lhs = mock[LogicalPlan]
    val pq = PlannerQuery.empty
    val result = codeUndertest.apply(pq, lhs, head = false)

    verify(lpp, never()).planEager(any())
  }

  test("read one node and create one in a tail") {
    // given
    val fakeUpdate = mock[LogicalPlan]
    val codeUndertest = PlanEagerness(FakePlanner(fakeUpdate))
    val lhs = mock[LogicalPlan]
    val pq = RegularPlannerQuery(QueryGraph(patternNodes = Set('a), mutatingPatterns = Seq(createNode("a"))))

    // when
    val result = codeUndertest.apply(pq, lhs, head = false)

    // then
    verify(lpp).planEager(lhs)
  }

  test("read one node and create one in head") {
    val codeUndertest = PlanEagerness(FakePlanner(mock[LogicalPlan]))
    val lhs = AllNodesScan('a, Set.empty)(null)
    val pq = RegularPlannerQuery(QueryGraph(patternNodes = Set('a), mutatingPatterns = Seq(createNode("a"))))
    val result = codeUndertest.apply(pq, lhs, head = true)

    verify(lpp, never()).planEager(any())
  }

  test("read two nodes and create one in head") {
    // given
    implicit val context = mock[LogicalPlanningContext]
    val lpp = mock[LogicalPlanProducer]
    when(context.logicalPlanProducer).thenReturn(lpp)
    val codeUndertest = PlanEagerness(FakePlanner(mock[LogicalPlan]))
    val lhs = CartesianProduct(AllNodesScan('a, Set.empty)(null), AllNodesScan('b, Set.empty)(null))(null)
    val pq = RegularPlannerQuery(QueryGraph(patternNodes = Set('a, 'b), mutatingPatterns = Seq(createNode("a"))))

    // when
    val result = codeUndertest.apply(pq, lhs, head = true)

    // then
    verify(lpp).planEager(lhs)
  }

  private def createNode(name: String) = CreateNodePattern(IdName(name), Seq.empty, None)
}

case class FakePlanner(result: LogicalPlan)
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = result
}
