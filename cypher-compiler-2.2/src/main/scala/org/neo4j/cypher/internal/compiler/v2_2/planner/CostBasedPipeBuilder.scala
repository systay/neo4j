/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.conditions.containsNamedPathOnlyForShortestPath
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{PipeBuilder, PipeInfo}
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.{ApplyRewriter, RewriterCondition, RewriterStepSequencer}
import org.neo4j.helpers.Clock

/* This class is responsible for taking a query from an AST object to a runnable object.  */
case class CostBasedPipeBuilder(monitors: Monitors,
                                metricsFactory: MetricsFactory,
                                monitor: PlanningMonitor,
                                clock: Clock,
                                tokenResolver: SimpleTokenResolver,
                                executionPlanBuilder: PipeExecutionPlanBuilder,
                                queryPlanner: QueryPlanner,
                                queryGraphSolver: QueryGraphSolver,
                                plannerName: CostBasedPlannerName)
  extends PipeBuilder {

  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo = {
    val statement = CostBasedPipeBuilder.rewriteStatement(inputQuery.statement, inputQuery.scopeTree,
                                                          inputQuery.semanticTable, inputQuery.conditions,
                                                          monitors.newMonitor[AstRewritingMonitor]())
    statement match {
      case (ast: Query, rewrittenSemanticTable) =>
        monitor.startedPlanning(inputQuery.queryText)
        val (logicalPlan, pipeBuildContext) = produceLogicalPlan(ast, rewrittenSemanticTable)(planContext)
        monitor.foundPlan(inputQuery.queryText, logicalPlan)
        val result = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, planContext)
        monitor.successfulPlanning(inputQuery.queryText, result)
        result

      case _ =>
        throw new CantHandleQueryException
    }
  }

  def produceLogicalPlan(ast: Query, semanticTable: SemanticTable)(planContext: PlanContext): (LogicalPlan, PipeExecutionBuilderContext) = {
    tokenResolver.resolve(ast)(semanticTable, planContext)
    val unionQuery = ast.asUnionQuery

    val metrics = metricsFactory.newMetrics(planContext.statistics)
    val logicalPlanProducer = LogicalPlanProducer(metrics.cardinality)
    val context = LogicalPlanningContext(planContext, logicalPlanProducer, metrics, semanticTable, queryGraphSolver)
    val plan = queryPlanner.plan(unionQuery)(context)

    val costPlannerName = plannerName match {
      case ConservativePlannerName => CostPlannerName
      case _                   => plannerName
    }

    val pipeBuildContext = PipeExecutionBuilderContext(metrics.cardinality, semanticTable, costPlannerName)

    (plan, pipeBuildContext)
  }
}

object CostBasedPipeBuilder {
  import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStep._

  def rewriteStatement(statement: Statement, scopeTree: Scope, semanticTable: SemanticTable, preConditions: Set[RewriterCondition], monitor: AstRewritingMonitor): (Statement, SemanticTable) = {
    val namespacer = Namespacer(statement, scopeTree)
    val newStatement = rewriteStatement(namespacer, statement, preConditions, monitor)
    val newSemanticTable = namespacer.tableRewriter(semanticTable)
    (newStatement, newSemanticTable)
  }

  private def rewriteStatement(namespacer: Namespacer, statement: Statement, preConditions: Set[RewriterCondition], monitor: AstRewritingMonitor): Statement = {
    val rewriter =
      RewriterStepSequencer
        .newDefault("Planner")
        .withPrecondition(preConditions)(
          ApplyRewriter("namespaceIdentifiers", namespacer.astRewriter),
          rewriteEqualityToInCollection,
          CNFNormalizer()(monitor),
          collapseInCollections,
          nameUpdatingClauses /* this is actually needed as a precondition for projectedNamedPaths even though we do not handle updates in Ronja */,
          projectNamedPaths,
          enableCondition(containsNamedPathOnlyForShortestPath),
          projectFreshSortExpressions
        ).rewriter

    statement.endoRewrite(rewriter)
  }
}

trait PlanningMonitor {
  def startedPlanning(q: String)
  def foundPlan(q: String, p: LogicalPlan)
  def successfulPlanning(q: String, p: PipeInfo)
}
