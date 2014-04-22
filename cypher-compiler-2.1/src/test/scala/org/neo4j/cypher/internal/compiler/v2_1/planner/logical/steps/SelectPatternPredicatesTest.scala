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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{LogicalPlanContext, PlanTransformer, PlanTable, CandidateList}
import org.mockito.Mockito._
import org.mockito.Matchers._

class SelectPatternPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {
  val dir = Direction.OUTGOING
  val types = Seq.empty[RelTypeName]
  val relName = "  UNNAMED1"
  val nodeName = "  UNNAMED2"
  val patternRel = PatternRelationship(relName, ("a", nodeName), dir, types, SimplePatternLength)

  // MATCH (a) WHERE (a)-->()
  val exp: PatternExpression = PatternExpression(RelationshipsPattern(RelationshipChain(
    NodePattern(Some(Identifier("a")(pos)), Seq(), None, naked = false) _,
    RelationshipPattern(Some(Identifier(relName)(pos)), optional = false, types, None, None, dir) _,
    NodePattern(Some(Identifier(nodeName)(pos)), Seq(), None, naked = false) _
  ) _) _)

  val factory = newMockedMetricsFactory
  when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
    case _ => 1000.0
  })

  val passThrough = new PlanTransformer {
    def apply(input: LogicalPlan)(implicit context: LogicalPlanContext) = input
  }

  test("should introduce semi apply for unsolved exclusive optional match") {
    // Given
    val predicate = Predicate(Set(IdName("a")), exp)
    val selections = Selections(Set(predicate))
    val patternQG = QueryGraph().
        add(patternRel).
        addArgumentId(Seq(IdName("a"))).
        addCoveredIdsAsProjections()

    val exists = Exists(predicate, patternQG)
    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections,
      subQueries = Seq(exists)
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(newMockedStatistics)
    )


    val aPlan = newMockedLogicalPlan("a")
    val inner: Expand = Expand(SingleRow(Set(IdName("a"))), IdName("a"), dir, types, IdName(nodeName), IdName(relName), SimplePatternLength)(patternRel)

    // When
    val result = selectPatternPredicates(passThrough)(aPlan)

    // Then
    result should equal(SemiApply(aPlan, inner)(exists))
  }

  test("should not introduce semi apply for unsolved exclusive optional match when nodes not applicable") {
    // Given
    val predicate = Predicate(Set(IdName("a")), exp)
    val selections = Selections(Set(predicate))
    val patternQG = QueryGraph().
        add(patternRel).
        addArgumentId(Seq(IdName("a"))).
        addCoveredIdsAsProjections()

    val qg = QueryGraph(
      patternNodes = Set("b"),
      selections = selections,
      subQueries = Seq(Exists(predicate, patternQG))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg,
      metrics = factory.newMetrics(newMockedStatistics)
    )

    val bPlan = newMockedLogicalPlan("b")
    // When
    val result = selectPatternPredicates(passThrough)(bPlan)

    // Then
    result should equal(bPlan)
  }
}
