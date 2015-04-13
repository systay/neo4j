package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.Rewriter
import org.neo4j.cypher.internal.compiler.v2_3.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LazyMode, LogicalPlan, ProduceResult, Projection}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, LogicalPlanningTestSupport2, PlannerQuery, QueryGraph, RegularQueryProjection, SemanticTable, UnionQuery}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class DefaultQueryPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("adds ProduceResult with a single node") {
    val result = createProduceResultOperator(Set("a"), SemanticTable().addNode(ident("a")))

    result.nodes should equal(Seq("a"))
    result.relationships shouldBe empty
    result.other shouldBe empty
  }

  test("adds ProduceResult with a single relationship") {
    val result = createProduceResultOperator(Set("r"), SemanticTable().addRelationship(ident("r")))

    result.relationships should equal(Seq("r"))
    result.nodes shouldBe empty
    result.other shouldBe empty
  }

  test("adds ProduceResult with a single value") {
    val semanticTable = SemanticTable(

    )
    val result = createProduceResultOperator(Set("foo"), SemanticTable().addRelationship(ident("r")))

    result.relationships should equal(Seq("r"))
    result.nodes shouldBe empty
    result.other shouldBe empty
  }

  private def createProduceResultOperator(columns: Set[String], semanticTable: SemanticTable): ProduceResult = {
    implicit val planningContext = mockLogicalPlanningContext(semanticTable)

    val inputPlan = mock[LogicalPlan]
    when(inputPlan.availableSymbols).thenReturn(columns.map(IdName.apply))

    val queryPlanner = DefaultQueryPlanner(identity, planSingleQuery = new FakePlanner(inputPlan))

    val pq = PlannerQuery(horizon = RegularQueryProjection(columns.map(c => c -> Identifier(c)(pos)).toMap))

    val union = UnionQuery(Seq(pq), distinct = false)

    val result = queryPlanner.plan(union)

    result shouldBe a [ProduceResult]

    result.asInstanceOf[ProduceResult]
  }

  test("should set strictness when needed") {
    // given
    val plannerQuery = mock[PlannerQuery with CardinalityEstimation]
    when(plannerQuery.preferredStrictness).thenReturn(Some(LazyMode))
    when(plannerQuery.graph).thenReturn(QueryGraph.empty)
    when(plannerQuery.horizon).thenReturn(RegularQueryProjection())
    when(plannerQuery.tail).thenReturn(None)

    val lp = {
      val plan = mock[Projection]
      when(plan.availableSymbols).thenReturn(Set.empty[IdName])
      when(plan.solved).thenReturn(plannerQuery)
      plan
    }

    val context = mock[LogicalPlanningContext]
    when(context.input).thenReturn(QueryGraphSolverInput.empty)
    when(context.strategy).thenReturn(new QueryGraphSolver with PatternExpressionSolving {
      override def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = lp
    })
    when(context.withStrictness(any())).thenReturn(context)
    val producer = mock[LogicalPlanProducer]
    when(producer.planStarProjection(any(), any())(any())).thenReturn(lp)
    when(context.logicalPlanProducer).thenReturn(producer)
    val queryPlanner = new DefaultQueryPlanner(planRewriter = Rewriter.noop,
      planSingleQuery = planSingleQueryX(expressionRewriterFactory = (lpc) => Rewriter.noop ))

    // when
    val query = UnionQuery(Seq(plannerQuery), distinct = false)
    queryPlanner.plan(query)(context)

    // then
    verify(context, times(1)).withStrictness(LazyMode)
  }

  class FakePlanner(result: LogicalPlan) extends LogicalPlanningFunction1[PlannerQuery, LogicalPlan] {
    def apply(input: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = result
  }

  private def mockLogicalPlanningContext(semanticTable: SemanticTable) = LogicalPlanningContext(
    planContext = mock[PlanContext],
    logicalPlanProducer = LogicalPlanProducer(mock[Metrics.CardinalityModel]),
    metrics = mock[Metrics],
    semanticTable = semanticTable,
    strategy = mock[QueryGraphSolver])
}
