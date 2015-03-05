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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_2.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.greedy.{GreedyPlanTable, PlanTable}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{applyOptional, outerHashJoin, pickBestPlan}

import scala.annotation.tailrec


/*
 * This planner is based on the paper "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 * written by Donald Kossmann and Konrad Stocker
 *
 * Line comments correspond to the lines in the pseudo-code in that paper for the IDP1 algorithm.
 */
case class IDPQueryGraphSolver(maxDepth: Int = 9,
                               leafPlanFinder: LogicalLeafPlan.Finder = leafPlanOptions,
                               bestPlanFinder: CandidateSelector = pickBestPlan,
                               config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                               solvers: Seq[IDPTableSolver] = Seq(joinTableSolver, expandTableSolver),
                               optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin))
  extends QueryGraphSolver with PatternExpressionSolving {

  // TODO: For selection, for now
  override def emptyPlanTable: PlanTable = GreedyPlanTable.empty

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    implicit val kitFactory = kitFactoryWithShortestPathSupport(config.kitInContext)
    val components = queryGraph.connectedComponents
    val plans = if (components.isEmpty) planEmptyComponent(queryGraph) else planComponents(components)

    implicit val kit = kitFactory(queryGraph)
    val plansWithRemainingOptionalMatches = plans.map { (plan: LogicalPlan) => (plan, queryGraph.optionalMatches) }
    val result = connectComponents(plansWithRemainingOptionalMatches)
    result
  }

  // TODO: Get rid off
  private def kitFactoryWithShortestPathSupport(kitFactory: QueryGraph => QueryPlannerKit) = (qg: QueryGraph) => {
    val oldKit = kitFactory(qg)
    oldKit.copy(
      select = { (plan: LogicalPlan) =>
        qg.shortestPathPatterns.foldLeft(oldKit.select(plan)) { (plan, sp) =>
          if (sp.isFindableFrom(plan.availableSymbols))
            oldKit.select(planShortestPaths(plan, sp))
          else
            plan
        }
      }
    )
  }

  // TODO: Clean up
  private def connectComponents(plans: Seq[(LogicalPlan, Seq[QueryGraph])])(implicit context: LogicalPlanningContext, kit: QueryPlannerKit): LogicalPlan = {
    @tailrec
    def recurse(plans: Seq[(LogicalPlan, Seq[QueryGraph])]): LogicalPlan = {
      if (plans.size == 1) {
        val (resultPlan, leftOvers) = applyApplicableOptionalMatches(plans.head)
        if (leftOvers.nonEmpty)
          throw new InternalException(s"Failed to plan all optional matches:\n$leftOvers")
        resultPlan
      } else {
        val candidates = plans.map(applyApplicableOptionalMatches)
        val cartesianProducts: Map[LogicalPlan, (Set[(LogicalPlan, Seq[QueryGraph])], Seq[QueryGraph])] = (for (
            lhs @ (left, leftRemaining) <- candidates.iterator;
            rhs @ (right, rightRemaining) <- candidates.iterator if left ne right;
            // TODO: Test this line
            remaining = if (leftRemaining.size < rightRemaining.size) leftRemaining else rightRemaining;
            oldPlans = Set(lhs, rhs);
            newPlan = kit.select(planCartesianProduct(left, right))
          )
          yield newPlan ->(oldPlans, remaining)
        ).toMap
        val bestCartesian = bestPlanFinder(cartesianProducts.keys).get
        val (oldPlans, remaining) = cartesianProducts(bestCartesian)
        val newPlans = plans.filterNot(oldPlans.contains) :+ (bestCartesian -> remaining)
        recurse(newPlans)
      }
    }

    @tailrec
    def applyApplicableOptionalMatches(todo: (LogicalPlan, Seq[QueryGraph])): (/* new plan*/ LogicalPlan, /* remaining */ Seq[QueryGraph]) = {
      todo match {
        case (plan, allRemaining @ Seq(nextOptional, nextRemaining@_*)) => withOptionalMatch(plan, nextOptional) match {
          case Some(newPlan) => applyApplicableOptionalMatches(newPlan, nextRemaining)
          case None          => (plan, allRemaining)
        }

        case done =>
          done
      }
    }

    def withOptionalMatch(plan: LogicalPlan, optionalMatch: QueryGraph): Option[LogicalPlan] =
      if ((optionalMatch.argumentIds -- plan.availableSymbols).isEmpty) {
        val candidates = config.optionalSolvers.flatMap { solver => solver(optionalMatch, plan) }
        val best = bestPlanFinder(candidates)
        best
      } else {
        None
      }

    recurse(plans)
  }

  private def planComponents(components: Seq[QueryGraph])(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kitFactory: (QueryGraph) => QueryPlannerKit): Seq[LogicalPlan] =
    components.map { qg => planComponent(qg, kitFactory(qg)) }

  private def planEmptyComponent(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kitFactory: (QueryGraph) => QueryPlannerKit): Seq[LogicalPlan] = {
    val plan = if (queryGraph.argumentIds.isEmpty) planSingleRow() else planQueryArgumentRow(queryGraph)
    Seq(kitFactory(queryGraph).select(plan))
  }

  private def planComponent(initialQg: QueryGraph, kit: QueryPlannerKit)(implicit context: LogicalPlanningContext, leafPlanWeHopeToGetAwayWithIgnoring: Option[LogicalPlan]): LogicalPlan = {
    // TODO: Investigate dropping leafPlanWeHopeToGetAwayWithIgnoring argument
    val leaves = leafPlanFinder(config, initialQg)
    val sharedPatterns = leaves.map(_.solved.graph.patternRelationships).reduceOption(_ intersect _).getOrElse(Set.empty)
    val qg = initialQg.withoutPatternRelationships(sharedPatterns)

    if (qg.patternRelationships.size > 0) {
      // line 1-4
      val table = initTable(qg, kit, leaves)

      val initialToDo = Solvables(qg)
      val solutionGenerator = newSolutionGenerator(qg, kit, table)
      solvePatterns(qg, initialToDo, kit, table, solutionGenerator)

      // TODO: Not exactly pretty
      table.head
    } else {
      val solutionPlans = leaves.filter(plan => (qg.coveredIds -- plan.availableSymbols).isEmpty)
      bestPlanFinder(solutionPlans).getOrElse(throw new InternalException("Found no leaf plan for connected component.  This must not happen."))
    }
  }

  @tailrec
  private def solvePatterns(qg: QueryGraph, toDo: Set[Solvable], kit: QueryPlannerKit, table: IDPPlanTable,
                            solutionGenerator: Set[Solvable] => Iterable[LogicalPlan])
                           (implicit context: LogicalPlanningContext): Unit = {
    val size = toDo.size
    if (size > 1) {
      // line 7-16
      val k = Math.min(size, maxDepth)
      for (i <- 2 to k;
           goal <- toDo.subsets(i) if !table.contains(goal); // If we already have an optimal plan, no need to replan
           candidates = solutionGenerator(goal);
           best <- bestPlanFinder(candidates)) {
        table.put(goal, best)
      }

      // line 17
      val blockCandidates = table.plansOfSize(k)
      val (bestSolvables, bestBlock) = pickSolution(blockCandidates).getOrElse(throw new InternalException("Did not find a single solution for a block"))

      // TODO: Test this
      // line 18 - 21
      val blockSolved = SolvableBlock(bestSolvables.flatMap(_.solvables))
      table.put(Set(blockSolved), bestBlock)
      val newToDo = toDo -- bestSolvables + blockSolved
      bestSolvables.subsets.foreach(table.remove)
      solvePatterns(qg, newToDo, kit, table, solutionGenerator)
    }
  }

  def initTable(qg: QueryGraph, kit: QueryPlannerKit, leaves: Set[LogicalPlan])(implicit context: LogicalPlanningContext): IDPPlanTable = {
    val table = new IDPPlanTable
    qg.patternRelationships.foreach { pattern =>
      val accessPlans = planSinglePattern(qg, pattern, leaves).map(kit.select)
      val bestAccessor = bestPlanFinder(accessPlans).getOrElse(throw new InternalException("Found no access plan for a pattern relationship in a connected component. This must not happen."))
      table.put(Set(SolvableRelationship(pattern)), bestAccessor)
    }
    table
  }

  private def pickSolution(input: Iterable[(Set[Solvable], LogicalPlan)])(implicit context: LogicalPlanningContext): Option[(Set[Solvable], LogicalPlan)] =
    bestPlanFinder[(Set[Solvable], LogicalPlan)](_._2, input)

  private def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Iterable[LogicalPlan]): Iterable[LogicalPlan] = {

    import expandTableSolver.planSinglePatternSide

    leaves.collect {
      case plan =>
        val (start, end) = pattern.nodes
        val leftPlan = planSinglePatternSide(qg, pattern, plan, start)
        val rightPlan = planSinglePatternSide(qg, pattern, plan, end)
        leftPlan.toSet ++ rightPlan.toSet
    }.flatten
  }

  private def newSolutionGenerator(qg: QueryGraph, kit: QueryPlannerKit, table: IDPPlanTable): (Set[Solvable]) => Iterable[LogicalPlan] = {
    val solverFunctions = solvers.map { solver => (goal: Set[Solvable]) => solver(qg, goal, table)}
    val solutionGenerator = (goal: Set[Solvable]) => solverFunctions.flatMap { solver => solver(goal).map(kit.select) }
    solutionGenerator
  }
}

