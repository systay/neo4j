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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan, ProduceResult}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.{aggregation, projection, sortSkipAndLimit, verifyBestPlan}
import org.neo4j.cypher.internal.compiler.v2_3.symbols._

import scala.collection.mutable

trait QueryPlanner {
  def plan(plannerQuery: UnionQuery)(implicit context: LogicalPlanningContext): LogicalPlan
}

case class DefaultQueryPlanner(planRewriter: Rewriter,
                               config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                               planSingleQuery: LogicalPlanningFunction1[PlannerQuery, LogicalPlan] = planSingleQueryX())
  extends QueryPlanner {

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext): LogicalPlan = unionQuery match {
    case UnionQuery(queries, distinct) =>
      val plan = planQueries(queries, distinct)
      val rewrittenPlan = plan.endoRewrite(planRewriter)

      val nodes = mutable.ListBuffer[String]()
      val rels = mutable.ListBuffer[String]()
      val others = mutable.ListBuffer[String]()

      val lastQuery = unionQuery.queries.last
      val columns = lastQuery.lastQueryHorizon.exposedSymbols(lastQuery.lastQueryGraph)

      columns.foreach {
        case IdName(name) =>
          if (context.semanticTable.getTypeFor(name) == CTNode.invariant)
            nodes += name
          else if (context.semanticTable.getTypeFor(name) == CTRelationship.invariant)
            rels += name
          else
            others += name
      }

      ProduceResult(nodes, rels, others, rewrittenPlan)

    case _ =>
      throw new CantHandleQueryException
  }

  private def planQueries(queries: Seq[PlannerQuery], distinct: Boolean)(implicit context: LogicalPlanningContext) = {
    val logicalPlans: Seq[LogicalPlan] = queries.map(p => planSingleQuery(p))
    val unionPlan = logicalPlans.reduce[LogicalPlan] {
      case (p1, p2) => context.logicalPlanProducer.planUnion(p1, p2)
    }

    if (distinct)
      context.logicalPlanProducer.planDistinct(unionPlan)
    else
      unionPlan
  }
}

case class planEventHorizonX(config: QueryPlannerConfiguration = QueryPlannerConfiguration.default)
  extends LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] {

  override def apply(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = config.applySelections(plan, query.graph)
    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection)
        sortSkipAndLimit(aggregationPlan, query)

      case queryProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query)
        val x = projection(sortedAndLimited, queryProjection.projections)
         x

      case UnwindProjection(identifier, expression) =>
        context.logicalPlanProducer.planUnwind(plan, identifier, expression)

      case _ =>
        throw new CantHandleQueryException
    }

    projectedPlan
  }
}

case class planWithTailX(expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                        planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = planEventHorizonX())
  extends LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] {

  override def apply(pred: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan = {
    remaining match {
      case Some(query) =>
        val lhs = pred
        val lhsContext = context.recurse(lhs)
        val rhs = planPart(query, lhsContext, Some(context.logicalPlanProducer.planQueryArgumentRow(query.graph)))
        val applyPlan = context.logicalPlanProducer.planTailApply(lhs, rhs)

        val applyContext = lhsContext.recurse(applyPlan)
        val projectedPlan = planEventHorizon(query, applyPlan)(applyContext)

        val projectedContext = applyContext.recurse(projectedPlan)
        val expressionRewriter = expressionRewriterFactory(projectedContext)
        val completePlan = projectedPlan.endoRewrite(expressionRewriter)

        // planning nested expressions doesn't change outer cardinality
        apply(completePlan, query.tail)(projectedContext)

      case None =>
        pred
    }
  }
}

case object planPart extends ((PlannerQuery, LogicalPlanningContext, Option[LogicalPlan]) => LogicalPlan) {

  def apply(query: PlannerQuery, context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    val ctx = query.preferredStrictness match {
      case Some(mode) if !context.input.strictness.exists(mode == _) => context.withStrictness(mode)
      case _ => context
    }
    ctx.strategy.plan(query.graph)(ctx, leafPlan)
  }
}

case class planSingleQueryX(planPart: (PlannerQuery, LogicalPlanningContext, Option[LogicalPlan]) => LogicalPlan = planPart,
                           planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = planEventHorizonX(),
                           expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                           planWithTail: LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] = planWithTailX()) extends LogicalPlanningFunction1[PlannerQuery, LogicalPlan] {

  override def apply(in: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val partPlan = planPart(in, context, None)

    val projectedPlan = planEventHorizon(in, partPlan)
    val projectedContext = context.recurse(projectedPlan)
    val expressionRewriter = expressionRewriterFactory(projectedContext)
    val completePlan = projectedPlan.endoRewrite(expressionRewriter)

    val finalPlan = planWithTail(completePlan, in.tail)(projectedContext)
    verifyBestPlan(finalPlan, in)
  }
}
