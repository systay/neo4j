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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp.cartesianProductsOrValueJoins.T
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.planShortestPaths
import scala.annotation.tailrec

trait IDPQueryGraphSolverMonitor extends IDPSolverMonitor {
  def noIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit
  def initTableFor(graph: QueryGraph): Unit
  def startIDPIterationFor(graph: QueryGraph): Unit
  def endIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit
  def emptyComponentPlanned(graph: QueryGraph, plan: LogicalPlan): Unit
  def startConnectingComponents(graph: QueryGraph): Unit
  def endConnectingComponents(graph: QueryGraph, result: LogicalPlan): Unit
}

object IDPQueryGraphSolver {
  val VERBOSE = java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")
}

/**
 * This planner is based on the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
case class IDPQueryGraphSolver(singleComponentSolver: SingleComponentPlannerTrait,
                               cartesianProductsOrValueJoins: JoinDisconnectedQueryGraphComponents,
                               monitor: IDPQueryGraphSolverMonitor) extends QueryGraphSolver with PatternExpressionSolving {

  private implicit val x = singleComponentSolver

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    implicit val kit = kitWithShortestPathSupport(context.config.toKit())
    val components = queryGraph.connectedComponents
    val plans = if (components.isEmpty) planEmptyComponent(queryGraph) else planComponents(components)

    monitor.startConnectingComponents(queryGraph)
    val result = connectComponentsAndSolveOptionalMatch(plans.toSet, queryGraph)
    monitor.endConnectingComponents(queryGraph, result)
    result
  }

  private def kitWithShortestPathSupport(kit: QueryPlannerKit)(implicit context: LogicalPlanningContext) =
    kit.copy(select = selectShortestPath(kit, _, _))

  private def selectShortestPath(kit: QueryPlannerKit, initialPlan: LogicalPlan, qg: QueryGraph)
                                (implicit context: LogicalPlanningContext): LogicalPlan =
    qg.shortestPathPatterns.foldLeft(kit.select(initialPlan, qg)) {
      case (plan, sp) if sp.isFindableFrom(plan.availableSymbols) =>
        val shortestPath = planShortestPaths(plan, qg, sp)
        kit.select(shortestPath, qg)
      case (plan, _) => plan
    }

  private def planComponents(components: Seq[QueryGraph])(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kit: QueryPlannerKit): Seq[T] =
    components.map { qg =>
      qg -> singleComponentSolver.planComponent(qg)
    }

  private def planEmptyComponent(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kit: QueryPlannerKit): Seq[T] = {
    val plan = if (queryGraph.argumentIds.isEmpty)
      context.logicalPlanProducer.planSingleRow()
    else
      context.logicalPlanProducer.planQueryArgumentRow(queryGraph)
    val result: LogicalPlan = kit.select(plan, queryGraph)
    monitor.emptyComponentPlanned(queryGraph, result)
    Seq(queryGraph -> result)
  }

  private def connectComponentsAndSolveOptionalMatch(plans: Set[T], qg: QueryGraph)
                                                    (implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {

    @tailrec
    def recurse(plans: Set[T], optionalMatches: Seq[QueryGraph]): (Set[T], Seq[QueryGraph]) = {
      if (optionalMatches.nonEmpty) {
        // If we have optional matches left to solve - start with that
        val firstOptionalMatch = optionalMatches.head
        val applicablePlan = plans.find(p => firstOptionalMatch.argumentIds subsetOf p._2.availableSymbols)

        applicablePlan match {
          case Some(t@(solvedQg, p)) =>
            val candidates = context.config.optionalSolvers.flatMap(solver => solver(firstOptionalMatch, p))
            val best = kit.pickBest(candidates).get
            recurse(plans - t + (solvedQg -> best), optionalMatches.tail)

          case None =>
            // If we couldn't find any optional match we can take on, produce the best cartesian product possible
            recurse(cartesianProductsOrValueJoins(plans, qg), optionalMatches)
        }
      } else if (plans.size > 1) {

        recurse(cartesianProductsOrValueJoins(plans, qg), optionalMatches)
      } else (plans, optionalMatches)
    }

    val (resultingPlans, optionalMatches) = recurse(plans, qg.optionalMatches)
    assert(resultingPlans.size == 1)
    assert(optionalMatches.isEmpty)
    resultingPlans.head._2
  }
}

