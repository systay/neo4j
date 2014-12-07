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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans

import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.RelTypeId
import org.neo4j.cypher.internal.compiler.v2_2.ast.{PatternExpression, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Candidates
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.relationshipsByTypePlanner
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.graphdb.Direction

import scala.collection.mutable

class RelationshipByTypeLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]
  val relName       = IdName("r")
  val N             = IdName("n")
  val M             = IdName("m")
  val X             = IdName("x")
  val TYP1          = RelTypeName("TYPE")(pos)
  val TYP2          = RelTypeName("TYPE2")(pos)
  val pattern       = PatternRelationship(relName, (N, M), Direction.OUTGOING, Seq(TYP1), SimplePatternLength)
  val pattern2      = PatternRelationship(relName, (M, X), Direction.OUTGOING, Seq(TYP2), SimplePatternLength)
  val semanticTable = newMockedSemanticTable
  val typId         = RelTypeId(42)
  val typId2        = RelTypeId(45)
  when(semanticTable.resolvedRelTypeNames).thenReturn(mutable.Map("TYPE" -> typId, "TYPE2" -> typId2))
  val factory = newMockedMetricsFactory

  implicit val context = newMockedLogicalPlanningContext(
    semanticTable = semanticTable,
    planContext = newMockedPlanContext,
    metrics = factory.newMetrics(hardcodedStatistics, semanticTable)
  )

  test("no plans for QG with no relationships") {
    // given
    val queryGraph = QueryGraph(patternNodes = Set(N))

    // when
    val resultPlans = relationshipsByTypePlanner(queryGraph)

    // then
    resultPlans should equal(Candidates())
  }

  test("single rel by type") {
    // given
    val queryGraph = QueryGraph(
      patternNodes = Set(N, X),
      patternRelationships = Set(pattern)
    )

    // when
    val resultPlans = relationshipsByTypePlanner(queryGraph)

    // then
    resultPlans should equal(Candidates(
      planRelationshipsByTypeScan(N, M, pattern, Right(typId), Set.empty)
    ))
  }

  test("single rel by type - backwards!") {
    // given
    val queryGraph = QueryGraph(
      patternNodes = Set(N, M),
      patternRelationships = Set(pattern.copy(dir = Direction.INCOMING))
    )

    // when
    val resultPlans = relationshipsByTypePlanner(queryGraph)

    // then
    resultPlans should equal(Candidates(
      planRelationshipsByTypeScan(M, N, pattern, Right(typId), Set.empty)
    ))
  }

  test("too many types - no plan") {
    // given
    val queryGraph = QueryGraph(
      patternNodes = Set(N, X),
      patternRelationships = Set(pattern.copy(types = Seq(TYP1, TYP2)))
    )

    // when
    val resultPlans = relationshipsByTypePlanner(queryGraph)

    // then
    resultPlans should equal(Candidates())
  }

  test("no types - no plan") {
    // given
    val queryGraph = QueryGraph(
      patternNodes = Set(N, X),
      patternRelationships = Set(pattern.copy(types = Seq()))
    )

    // when
    val resultPlans = relationshipsByTypePlanner(queryGraph)

    // then
    resultPlans should equal(Candidates())
  }

  test("two relationships produces two plans") {
    // given
    val queryGraph = QueryGraph(
      patternNodes = Set(N, X),
      patternRelationships = Set(pattern, pattern2)
    )

    // when
    val resultPlans = relationshipsByTypePlanner(queryGraph)

    // then
    resultPlans should equal(Candidates(
      planRelationshipsByTypeScan(N, M, pattern, Right(typId), Set.empty),
      planRelationshipsByTypeScan(M, X, pattern2, Right(typId2), Set.empty)
    ))
  }
}
