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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CardinalityTestHelper
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence.{AssumeIndependenceQueryGraphCardinalityModel, IndependenceCombiner}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.helpers.testRandomizer

class AssumeIndependenceQueryGraphCardinalityModelTest extends CypherFunSuite with LogicalPlanningTestSupport with CardinalityTestHelper {


  // Glossary:
  val N: Double  = testRandomizer.nextDouble() * 1E6 // Graph node count - the god number.
  println("N: " + N)
  val Asel = .2          // How selective a :A predicate is
  val Bsel = .1          // How selective a :B predicate is
  val Csel = .01         // How selective a :C predicate is
  val Dsel = .001        // How selective a :D predicate is
  val A = N * Asel // Nodes with label A
  val B = N * Bsel // Nodes with label B
  val C = N * Csel // Nodes with label C
  val D = N * Dsel // Nodes with label D

  val Aprop = 0.5        // Selectivity of index on :A(prop)
  val Bprop = 0.003      // Selectivity of index on :B(prop)
  val Abar = 0.002       // Selectivity of index on :A(bar)

  val A_T1_A_sel: Double = 5.0 / A // Numbers of relationships of type T1 between A and B respectively labeled nodes
  val A_T1_B_sel = 0.5
  val A_T1_C_sel = 0.05
  val A_T1_D_sel = 0.005
  val A_T1_STAR_sel = or(A_T1_A_sel, A_T1_B_sel, A_T1_C_sel, A_T1_D_sel)

  val A_T1_A    = A * A * A_T1_A_sel
  val A_T1_B    = A * B * A_T1_B_sel
  val A_T1_C    = A * C * A_T1_C_sel
  val A_T1_D    = A * D * A_T1_D_sel
  val A_T1_STAR = A_T1_A + A_T1_B + A_T1_C + A_T1_D

  val B_T1_B_sel = 10.0 / B
  val B_T1_C_sel = 0.1
  val B_T1_A_sel = 0.01
  val B_T1_D_sel = 0.001
  val STAR_T1_A_sel = or(A_T1_A_sel, B_T1_A_sel)
  val STAR_T1_A = N * A * STAR_T1_A_sel

  val B_T1_B    = B * B * B_T1_B_sel
  val B_T1_C    = B * C * B_T1_C_sel
  val B_T1_A    = B * A * B_T1_A_sel
  val B_T1_D    = B * D * B_T1_D_sel
  val B_T1_STAR = B_T1_A + B_T1_B + B_T1_C + B_T1_D
  val STAR_T1_B = B_T1_B + A_T1_B

  val C_T1_D_sel= 0.02
  val C_T1_D    = C * D * C_T1_D_sel

  val D_T1_C_sel = 0.3
  val D_T1_C     = D * C * D_T1_C_sel

  val A_T2_A_sel = 0
  val A_T2_B_sel = 5

  val A_T2_A    = A * A * A_T2_A_sel
  val A_T2_B    = A * B * A_T2_B_sel
  val A_T2_STAR = A_T2_A + A_T2_B
  val STAR_T2_B = A_T2_B + 0 // B_T2_B

  val B_T2_C_sel = 0.0031
  val B_T2_C = B * C * B_T2_C_sel
  val D_T2_C_sel = 0.07
  val D_T2_C     = D * C * D_T2_C_sel

  // Relationship count: the total number of relationships in the system
  val R = A_T1_STAR + B_T1_STAR + A_T2_STAR + D_T1_C + D_T2_C

  import org.scalatest.prop.TableDrivenPropertyChecks._

  test("all queries") {
    val queries = Table.apply[String, Double](
      ("query", "expected cardinality"),
      "MATCH (n)"
        -> N,

      "MATCH (n:A)"
        -> A,

      "MATCH (n:A) MATCH (m:B)"
        -> A * B,

      "MATCH a, b" ->
        N * N,

      ""
        -> 1.0,

      "MATCH a, (b:B)"
        -> N * B,

      "MATCH (a:A:B)"
        -> N * Asel * Bsel,

      "MATCH (a:B:A)"
        -> N * Asel * Bsel,

      "MATCH (a:Z)"
        -> 0.0,

      "MATCH (a:A:Z)"
        -> 0.0,

      "MATCH (a:Z:B)"
        -> 0.0,

      "MATCH (a:A) WHERE a.prop = 42"
        -> A * Aprop,

      "MATCH (a:B) WHERE a.bar = 42"
        -> B * DEFAULT_EQUALITY_SELECTIVITY,

      "MATCH (a:A) WHERE NOT a.prop = 42"
        -> A * (1 - Aprop),

      "MATCH (a:A) WHERE a.prop = 42 OR a.prop = 43"
        -> A * or(Aprop, Aprop),

      "MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43"
        -> A * or(Aprop, Abar),

      "MATCH (a:B) WHERE a.prop = 42 OR a.bar = 43"
        -> B * or(Bprop, DEFAULT_EQUALITY_SELECTIVITY),

      "MATCH (a) WHERE false"
        -> 0,

      "MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43"
        -> A * Aprop * Abar,

      "MATCH (a:B) WHERE a.prop = 42 AND a.bar = 43"
        -> B * Bprop * DEFAULT_EQUALITY_SELECTIVITY,

      "MATCH (a)-->(b)"
        -> R,

      "MATCH (a:A)-[r:T1]->(b:B)"
        -> A_T1_B,

      "MATCH (b:B)<-[r:T1]-(a:A)"
        -> A_T1_B,

      "MATCH (a:A)-[r:T1]-(b:B)"
        -> A * B * or(A_T1_B_sel, B_T1_A_sel),

      "MATCH (a:A)-[r:T1]->(b)"
        -> A_T1_STAR,

      "MATCH (a:A)-[r:T1]-(b)"
        -> {
        val STAR_T1_A = B_T1_A
        A_T1_STAR + STAR_T1_A
      },
      "MATCH (a:A:B)-->()"
        -> {
        val maxRelCount = N * N * Asel * Bsel
        val B_T2_STAR = 0
        val A_relSelectivity = (A_T1_STAR + A_T2_STAR) / maxRelCount
        val B_relSelectivity = (B_T1_STAR + B_T2_STAR) / maxRelCount
        val relSelectivity = A_relSelectivity * B_relSelectivity
        A * B * relSelectivity
      },


      "MATCH (a:A)-[:T1|:T2]->(:B)"
        -> {
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Bsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelectivity = (A_T1_B + A_T2_B) / maxRelCount - (A_T1_B / maxRelCount) * (A_T2_B / maxRelCount)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a:A:B)-[:T1|:T2]->()"
        -> {
        val B_T2_STAR = 0
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Bsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelectivityT1 = (A_T1_STAR / maxRelCount) * (B_T1_STAR / maxRelCount)
        val relSelectivityT2 = (A_T2_STAR / maxRelCount) * (B_T2_STAR / maxRelCount)
        val relSelectivity = or(relSelectivityT1, relSelectivityT2)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a:A:D)-[:T1|:T2]->()"
        -> {
        val D_T1_STAR = D_T1_C
        val D_T2_STAR = D_T2_C
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Dsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelectivityT1 = (A_T1_STAR / maxRelCount) * (D_T1_STAR / maxRelCount)
        val relSelectivityT2 = (A_T2_STAR / maxRelCount) * (D_T2_STAR / maxRelCount)
        val relSelectivity = or(relSelectivityT1, relSelectivityT2)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a:A:B)-[:T1|:T2]->(c:C:D)"
        -> {
        val A_T2_C = 0
        val A_T2_D = 0
        val B_T2_C = 0
        val B_T2_D = 0
        val patternNodeCrossProduct = N * N
        val labelSelectivity = Asel * Bsel * Csel * Dsel
        val maxRelCount = patternNodeCrossProduct * labelSelectivity
        val relSelT1 = (A_T1_C / maxRelCount) * (A_T1_D / maxRelCount) * (B_T1_C / maxRelCount) * (B_T1_D / maxRelCount)
        val relSelT2 = (A_T2_C / maxRelCount) * (A_T2_D / maxRelCount) * (B_T2_C / maxRelCount) * (B_T2_D / maxRelCount)
        val relSelectivity = or(relSelT1, relSelT2)
        patternNodeCrossProduct * labelSelectivity * relSelectivity
      },

      "MATCH (a) OPTIONAL MATCH (a)-[:T1]->(:B)"
        -> (A_T1_B + B_T1_B),

      "MATCH (a:A) OPTIONAL MATCH (a)-[:T1]->(:B)"
        -> A_T1_B,

      "MATCH (a:A) OPTIONAL MATCH (a)-[:MISSING]->()"
        -> A,

      "MATCH (a) OPTIONAL MATCH (b)"
        -> N * N,

      "MATCH (a:A) OPTIONAL MATCH (b:B) OPTIONAL MATCH (c:C)"
        -> A * B * C,

      "MATCH (a:A) WHERE id(a) IN [1,2,3]"
        -> A * (3.0 / N),

      "MATCH (a:A)-[:T1]->(b:B)-[:T2]->(c:C)"
        -> N * N * N * Asel * A_T1_B_sel * Bsel * B_T2_C_sel * Csel,

      "MATCH (a:A)-[:T1]->(b:B)-[:T1]->(c:C)"
        -> N * N * N * Asel * A_T1_B_sel * Bsel * B_T1_C_sel * Csel *
        DEFAULT_REL_UNIQUENESS_SELECTIVITY,

      "MATCH (:A)-[:T1]->(:A)-[:T1]->(:B)-[:T1]->(:B)"
        -> A * A * B * B * A_T1_A_sel * A_T1_B_sel * B_T1_B_sel *
        Math.pow(DEFAULT_REL_UNIQUENESS_SELECTIVITY, 3), // Once per rel-uniqueness predicate

      "MATCH (:A)-[r1:T1]->(:A)-[r2:T1]->(:B)-[r3:T1]->(:B)-[r4:T2]->(c:C)"
        -> A * A * B * B * C * A_T1_A_sel * A_T1_B_sel * B_T1_B_sel * B_T2_C_sel *
        Math.pow(DEFAULT_REL_UNIQUENESS_SELECTIVITY, 4)

    )

    forAll(queries) { (q: String, expected: Double) =>
      forQuery(q).
        shouldHaveQueryGraphCardinality(expected)
    }
  }

  ignore("cardinality for property equality predicate when property name is unknown (does not exist in the store)") {
    // This should work
    forQuery("MATCH (a) WHERE a.unknownProp = 42").
      shouldHaveQueryGraphCardinality(0)
  }

  test("empty graph") {
    givenPattern("MATCH a WHERE a.prop = 10").
      withGraphNodes(0).
      withKnownProperty('prop).
      shouldHaveQueryGraphCardinality(0)
  }

  ignore("honours bound arguments") {
    givenPattern("MATCH (a:FOO)-[:TYPE]->(b:BAR)").
    withQueryGraphArgumentIds(IdName("a")).
    withInboundCardinality(13.0).
    withGraphNodes(500).
    withLabel('FOO -> 100).
    withLabel('BAR -> 400).
    withRelationshipCardinality('FOO -> 'TYPE -> 'BAR -> 1000).
    shouldHaveQueryGraphCardinality(1000.0 / 500.0 * 13.0)
  }


  // TODO: Add a test for a relpatterns where the number of matching nodes is zero


  ignore("varlength two steps out") {
    forQuery("MATCH (a:A)-[r:T1*1..2]->(b:B)").
    shouldHaveQueryGraphCardinality(
        A_T1_A + // The result includes all (:A)-[:T1]->(:B)
          A * N * B * A_T1_STAR_sel * STAR_T1_A_sel * DEFAULT_REL_UNIQUENESS_SELECTIVITY // and all (:A)-[:T1]->()-[:T1]->(:B)
      )
  }

//  test("varlength three steps out") {
//    forQuery("MATCH (a:A)-[r:T1*1..3]->(b:B)").
//      shouldHaveQueryGraphCardinality(
//        A * B * A_T1_A_sel + // The result includes all (:A)-[:T1]->(:B)
//        A * N * B * A_T1_STAR_sel * STAR_T1_B_sel + // and all (:A)-[:T1]->()-[:T1]->(:B)
//        A * N * N * B * A_T1_STAR_sel * STAR_T1_STAR_sel * STAR_T1_B_sel  // and all (:A)-[:T1]->()-[:T1]->()-[:T1]-(:B)
//      )
//  }

  private def forQuery(q: String) =
    givenPattern(q).
    withGraphNodes(N).
    withLabel('A, A).
    withLabel('B, B).
    withLabel('C, C).
    withLabel('D, D).
    withLabel('EMPTY, 0).
    withIndexSelectivity(('A, 'prop) -> Aprop).
    withIndexSelectivity(('B, 'prop) -> Bprop).
    withIndexSelectivity(('A, 'bar) -> Abar).
    withRelationshipCardinality('A -> 'T1 -> 'A, A_T1_A).
    withRelationshipCardinality('A -> 'T1 -> 'B, A_T1_B).
    withRelationshipCardinality('A -> 'T1 -> 'C, A_T1_C).
    withRelationshipCardinality('A -> 'T1 -> 'D, A_T1_D).
    withRelationshipCardinality('A -> 'T2 -> 'A, A_T2_A).
    withRelationshipCardinality('A -> 'T2 -> 'B, A_T2_B).
    withRelationshipCardinality('B -> 'T1 -> 'B, B_T1_B).
    withRelationshipCardinality('B -> 'T1 -> 'C, B_T1_C).
    withRelationshipCardinality('B -> 'T1 -> 'A, B_T1_A).
    withRelationshipCardinality('B -> 'T1 -> 'D, B_T1_D).
    withRelationshipCardinality('B -> 'T2 -> 'C, B_T2_C).
    withRelationshipCardinality('C -> 'T1 -> 'D, C_T1_D).
    withRelationshipCardinality('D -> 'T1 -> 'C, D_T1_C).
    withRelationshipCardinality('D -> 'T2 -> 'C, D_T2_C)

  def or(numbers: Double*) = 1 - numbers.map(1 - _).reduce(_ * _)

  implicit def toLong(d: Double): Long = d.toLong

  def createCardinalityModel(stats: GraphStatistics, semanticTable: SemanticTable): QueryGraphCardinalityModel =
    AssumeIndependenceQueryGraphCardinalityModel(stats, semanticTable, IndependenceCombiner)
}
