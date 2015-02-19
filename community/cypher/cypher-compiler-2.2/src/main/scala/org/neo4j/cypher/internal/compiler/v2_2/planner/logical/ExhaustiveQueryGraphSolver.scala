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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{LogicalPlan, _}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{applyOptional, outerHashJoin, pickBestPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.RichQueryGraph._

import scala.annotation.tailrec

case class ExhaustiveQueryGraphSolver(leafPlanTableGenerator: PlanTableGenerator,
                                      planProducers: Seq[PlanProducer],
                                      bestPlanFinder: CandidateSelector,
                                      config: PlanningStrategyConfiguration,
                                      optionalSolvers: Seq[OptionalSolver])
  extends QueryGraphSolver with PatternExpressionSolving {
  def emptyPlanTable: PlanTable = ExhaustivePlanTable.empty

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    val (planTable, resultPlan) = solveMandatoryQuerygraph(queryGraph)

    val plan = resultPlan match {
      case Some(p) => p
      case None if queryGraph.argumentIds.nonEmpty => planQueryArgumentRow(queryGraph)
      case None => planSingleRow()
    }

    val optionalQGs = findQGsToSolve(plan, planTable, queryGraph.optionalMatches)
    solveOptionalQGsInOrder(plan, optionalQGs)
  }

  private def solveOptionalQGsInOrder(plan: LogicalPlan, optionalQGs: Seq[QueryGraph])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val result = optionalQGs.foldLeft(plan) {
      case (lhs: LogicalPlan, optionalQg: QueryGraph) =>
        val plans = optionalSolvers.flatMap(_.apply(optionalQg, lhs))
        assert(plans.map(_.solved).distinct.size == 1) // All plans are solving the same query
        bestPlanFinder(plans).get
    }
    result
  }

  // This is the dynamic programming part of the solver
  private def solveMandatoryQuerygraph(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): (PlanTable, Option[LogicalPlan]) = {
    val cache = initiateCacheWithLeafPlans(queryGraph, leafPlan)
    val plans = queryGraph.connectedComponents.map { qg =>
      (1 to qg.size) foreach { x =>
        qg.combinations(x).foreach {
          subQG =>
            val logicalPlans = planProducers.flatMap(producer => producer(subQG, cache))
            val plans = logicalPlans.map(config.applySelections(_, subQG))
            val bestPlan = bestPlanFinder(plans)
            bestPlan.foreach(p => cache + p)
        }
      }

      cache(qg)
    }

    val resultPlan = plans.reduceRightOption[LogicalPlan] { case (plan, acc) =>
      val result = config.applySelections(planCartesianProduct(plan, acc), queryGraph)
      cache + result
      result
    }
    (cache, resultPlan)
  }

  private def initiateCacheWithLeafPlans(queryGraph: QueryGraph, leafPlan: Option[LogicalPlan])
                                        (implicit context: LogicalPlanningContext) =
    leafPlanTableGenerator.apply(queryGraph, leafPlan).plans.foldLeft(context.strategy.emptyPlanTable)(_ + _)

  private def findQGsToSolve(plan: LogicalPlan, table: PlanTable, graphs: Seq[QueryGraph]): Seq[QueryGraph] = {
    @tailrec
    def inner(in: Seq[QueryGraph], out: Seq[QueryGraph]): Seq[QueryGraph] = in match {
      case hd :: tl if isSolved(table, hd)  => inner(tl, out)
      case hd :: tl if applicable(plan, hd) => inner(tl, out :+ hd)
      case _                                => out
    }

    inner(graphs, Seq.empty)
  }

  private def isSolved(table: PlanTable, optionalQG: QueryGraph) =
    table.plans.exists(_.solved.lastQueryGraph.optionalMatches.contains(optionalQG))

  private def applicable(outerPlan: LogicalPlan, optionalQG: QueryGraph) =
    optionalQG.argumentIds.subsetOf(outerPlan.availableSymbols)
}

object ExhaustiveQueryGraphSolver {

  def withDefaults(leafPlanTableGenerator: PlanTableGenerator = LeafPlanTableGenerator(PlanningStrategyConfiguration.default),
                   planProducers: Seq[PlanProducer] = Seq(expandOptions, joinOptions),
                   bestPlanFinder: CandidateSelector = pickBestPlan,
                   config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                   optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin)) =
    new ExhaustiveQueryGraphSolver(leafPlanTableGenerator, planProducers, bestPlanFinder, config, optionalSolvers)
}
