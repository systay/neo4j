package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_3.ast.{Identifier, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, AllNodesScan, Projection, SingleRow}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery, RegularQueryProjection, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class PlanEventHorizonXTest extends CypherFunSuite {

  val pos = DummyPosition(1)
  implicit val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[Metrics.CardinalityModel]), mock[Metrics], SemanticTable(), mock[QueryGraphSolver])

  test("should not do projection if not necessary") {
    // Given
    val pq = PlannerQuery(horizon = RegularQueryProjection(Map("a" -> Identifier("a")(pos))))
    val inputPlan = AllNodesScan(IdName("a"), Set.empty)(CardinalityEstimation.lift(PlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = planEventHorizonX()(pq, inputPlan)

    // Then
    inputPlan should equal(producedPlan)
  }

  test("should do projection if necessary") {
    // Given
    val literal: SignedDecimalIntegerLiteral = SignedDecimalIntegerLiteral("42")(pos)
    val pq = PlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
    val inputPlan = SingleRow()(CardinalityEstimation.lift(PlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = planEventHorizonX()(pq, inputPlan)

    // Then
    producedPlan should equal(Projection(inputPlan, Map("a" -> literal))(CardinalityEstimation.lift(PlannerQuery(), Cardinality(1))))
  }
}
