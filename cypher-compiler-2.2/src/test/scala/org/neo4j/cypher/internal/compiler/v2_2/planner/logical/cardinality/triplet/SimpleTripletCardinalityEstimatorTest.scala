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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet.TripletQueryGraphCardinalityModel.NodeCardinalities
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.helpers.RandomizedTestSupport

import scala.language.implicitConversions

class SimpleTripletCardinalityEstimatorTest
  extends CypherFunSuite
  with RandomizedTestSupport
  with LogicalPlanningTestSupport
  with CardinalityTestHelper
  with ForumPostsCardinalityData {

  implicit val input = QueryGraphCardinalityInput.empty

  test("(a)-[r]->(b)") {
    import ForumPosts._

    val nodeCardinalities: NodeCardinalities = cardinalitiesMap(
      "a" -> N,
      "b" -> N
    )

    val estimates = ForumPosts.asTriplets("(a)-[r]->(b)", nodeCardinalities)

    estimates should equal(cardinalitiesMap(
      "r" -> R
    ))
  }

  test("(a)-[r]-(b)") {
    import ForumPosts._

    val nodeCardinalities: NodeCardinalities = cardinalitiesMap(
      "a" -> N,
      "b" -> N
    )

    val estimates = ForumPosts.asTriplets("(a)-[r]-(b)", nodeCardinalities)

    estimates should equal(cardinalitiesMap(
      "r" -> 2 * R
    ))
  }

  test("(a:Forum)-[r:MEMBER_IN]->(b:Forum:Person)") {
    import ForumPosts._

    val nodeCardinalities: NodeCardinalities = cardinalitiesMap(
      "a" -> N,
      "b" -> N
    )

    val estimates = ForumPosts.asTriplets("(a:Forum)-[r:MEMBER_IN]->(b:Forum:Person)", nodeCardinalities)

    estimates should equal(cardinalitiesMap(
      "r" -> degree(Forums_MEMBER_IN_Forums, Forums) * N
    ))
  }

  test("(a:Person)-[r:KNOWS]->(b:Person)") {
    import ForumPosts._

    val nodeCardinalities: NodeCardinalities = cardinalitiesMap(
      "a" -> 1,
      "b" -> N
    )

    val estimates = ForumPosts.asTriplets("(a:Person)-[r:KNOWS]->(b:Person)", nodeCardinalities)

    estimates should equal(cardinalitiesMap(
      "r" -> degree(Persons_KNOWS_Persons, Persons)
    ))
  }

  test("(a:Person)-[r:KNOWS]->(b:Person:Clown)") {
    val nodeCardinalities: NodeCardinalities = cardinalitiesMap(
      "a" -> 1,
      "b" -> N
    )

    val estimates = ForumPosts.asTriplets("(a:Person)-[r:KNOWS]->(b:Person:Clown)", nodeCardinalities)

    estimates should equal(cardinalitiesMap(
      "r" -> 0.0d
    ))
  }

  test("(a:Person)-[r:XXX]->(b:Person)") {
    val nodeCardinalities: NodeCardinalities = cardinalitiesMap(
      "a" -> 1,
      "b" -> N
    )

    val estimates = ForumPosts.asTriplets("(a:Person)-[r:XXX]->(b:Person)", nodeCardinalities)

    estimates should equal(cardinalitiesMap(
      "r" -> 0.0d
    ))
  }

  implicit class RichCardinalityData(cardinalityData: CardinalityData) {
    def asTriplets(q: String, nodeCardinalities: NodeCardinalities)(implicit input: QueryGraphCardinalityInput): Map[IdName, Cardinality] = {
      val testUnit: CardinalityTestHelper#TestUnit = cardinalityData.forQuery(TestUnit(s"MATCH $q"))
      val (stats, semanticTable) = testUnit.prepareTestContext
      val qg = testUnit.createQueryGraph()
      val converter = TripletConverter(qg, input, semanticTable)
      val estimator = SimpleTripletCardinalityEstimator(N, nodeCardinalities, stats)

      qg.patternRelationships.collect {
        case pattern =>
          val triplet = converter(pattern)
          val estimate = estimator(triplet)

          pattern.name -> estimate
      }.toMap
    }
  }

  def cardinalitiesMap(data: (String, Double)*): NodeCardinalities =
    data.collect { case (name, amount) => IdName(name) -> Cardinality(amount) }.toMap
}
