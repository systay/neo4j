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

import org.neo4j.cypher.internal.compiler.v2_2.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.ast.{AllIterablePredicate, FilterScope, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.ExhaustiveQueryGraphSolver.MAX_SEARCH_DEPTH
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{applyOptional, outerHashJoin, pickBestPlan}

import scala.annotation.tailrec

object ExhaustiveQueryGraphSolver {
  val MAX_SEARCH_DEPTH = 5

  // TODO: Make sure this is tested by extracting tests from greedy expand step
  def planSinglePatternSide(qg: QueryGraph, patternRel: PatternRelationship, plan: LogicalPlan, nodeId: IdName): Option[LogicalPlan] = {
    val availableSymbols = plan.availableSymbols
    if (availableSymbols(nodeId)) {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val overlapping = availableSymbols.contains(otherSide)
      val mode = if (overlapping) ExpandInto else ExpandAll

      patternRel.length match {
        case SimplePatternLength =>
          Some(planSimpleExpand(plan, nodeId, dir, otherSide, patternRel, mode))

        case length: VarPatternLength =>
          // TODO: Move selections out here (?)
          val availablePredicates = qg.selections.predicatesGiven(availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(patternRel.name.name))
              if identifier == relId || !innerPredicate.dependencies(relId) =>
              (identifier, innerPredicate) -> all
          }.unzip
          Some(planVarExpand(plan, nodeId, dir, otherSide, patternRel, predicates, allPredicates, mode))
      }
    } else {
      None
    }
  }
}

case class ExhaustiveQueryGraphSolver(leafPlanFinder: LogicalLeafPlan.Finder = leafPlanOptions,
                                      bestPlanFinder: CandidateSelector = pickBestPlan,
                                      config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                                      solvers: Seq[ExhaustiveTableSolver] = Seq(joinTableSolver, expandTableSolver),
                                      optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin))
  extends QueryGraphSolver with PatternExpressionSolving {

  import ExhaustiveQueryGraphSolver.planSinglePatternSide

  // TODO: For selection, for now
  override def emptyPlanTable: PlanTable = GreedyPlanTable.empty

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    implicit val kitFactory = kitFactoryWithShortestPathSupport(config.kitInContext)
    val components = queryGraph.connectedComponents
    val plans = if (components.isEmpty) planEmptyComponent(queryGraph) else planComponents(components)

    implicit val kit = kitFactory(queryGraph)
    val result = connectComponents(plans, queryGraph.optionalMatches)
    result
  }

  // TODO: Get rid off
  private def kitFactoryWithShortestPathSupport(kitFactory: QueryGraph => PlanningStrategyKit) = (qg: QueryGraph) => {
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

  @tailrec
  private def connectComponents(plans: Seq[LogicalPlan], optionalMatches: Seq[QueryGraph])(implicit context: LogicalPlanningContext, kit: PlanningStrategyKit): LogicalPlan = {
    if (plans.size == 1) {
      optionalMatches.foldLeft(plans.head) { (plan, qg) => withOptionalMatch(qg)(plan).get }
    } else {
      val (nextOptional, nextOptionals) = if (optionalMatches.nonEmpty) (optionalMatches.headOption, optionalMatches.tail) else (None, Seq.empty)
      val optionalPlans = nextOptional.map( qg => plans.flatMap(plan => withOptionalMatch(qg)(plan)) ).getOrElse(Seq.empty)
      val (candidates, newOptionals) = if (optionalPlans.isEmpty) (plans, optionalMatches) else (optionalPlans, nextOptionals)
      val cartesianProducts =
        (for( left <- candidates.iterator; right <- candidates.iterator if left != right )
         yield kit.select(planCartesianProduct(left, right)) -> Set(left, right)).toMap
      val bestCartesian = pickBestPlan(cartesianProducts.keys.iterator).get
      val oldPlans = cartesianProducts(bestCartesian)
      val newPlans = plans.filterNot(oldPlans) :+ bestCartesian

      connectComponents(newPlans, newOptionals)
    }
  }

  private def withOptionalMatch(optionalMatch: QueryGraph)(plan: LogicalPlan)(implicit context: LogicalPlanningContext): Option[LogicalPlan] =
    if ((optionalMatch.argumentIds -- plan.availableSymbols).isEmpty) {
      val candidates = config.optionalSolvers.flatMap { solver => solver(optionalMatch, plan) }
      val best = pickBestPlan(candidates.iterator)
      best
    } else {
      None
    }

  private def planComponents(components: Seq[QueryGraph])(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kitFactory: (QueryGraph) => PlanningStrategyKit): Seq[LogicalPlan] =
    components.map { qg => planComponent(qg, kitFactory(qg)) }

  private def planEmptyComponent(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan], kitFactory: (QueryGraph) => PlanningStrategyKit): Seq[LogicalPlan] = {
    val plan = if (queryGraph.argumentIds.isEmpty) planSingleRow() else planQueryArgumentRow(queryGraph)
    Seq(kitFactory(queryGraph).select(plan))
  }

  private def planComponent(initialQg: QueryGraph, kit: PlanningStrategyKit)(implicit context: LogicalPlanningContext, leafPlanWeHopeToGetAwayWithIgnoring: Option[LogicalPlan]): LogicalPlan = {
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
      bestPlanFinder(leaves.iterator).getOrElse(throw new InternalException("Found no leaf plan for connected component.  This must not happen."))
    }
  }

  @tailrec
  private def solvePatterns(qg: QueryGraph, toDo: Set[Solvable], kit: PlanningStrategyKit, table: ExhaustivePlanTable, solutionGenerator: Set[Solvable] => Iterator[LogicalPlan])(implicit context: LogicalPlanningContext): Unit = {
    val size = toDo.size
    if (size > 1) {
      // line 7-16
      val k = Math.min(size, MAX_SEARCH_DEPTH) // TODO: Inject as parameter
      for (i <- 2 to k;
           goal <- toDo.subsets(i);
           candidates = solutionGenerator(goal);
           best <- bestPlanFinder(candidates)) {
        table.put(goal, best)
      }

      // TODO: Get rid of map
      // line 17
      val blockCandidates = toDo.subsets(k).flatMap( set => table(set).map(_ -> set) ).toMap
      val bestBlock = bestPlanFinder(blockCandidates.keys.iterator).getOrElse(throw new InternalException("Did not find a single solution for a block"))
      val bestSolvables = blockCandidates(bestBlock)

      // TODO: Test this
      // line 18 - 21
      val blockSolved = SolvableBlock(bestSolvables.flatMap(_.solvables))
      table.put(Set(blockSolved), bestBlock)
      val newToDo = toDo -- bestSolvables + blockSolved
      bestSolvables.subsets.foreach(table.remove)
      solvePatterns(qg, newToDo, kit, table, solutionGenerator)
    }
  }

  def initTable(qg: QueryGraph, kit: PlanningStrategyKit, leaves: Set[LogicalPlan])(implicit context: LogicalPlanningContext): ExhaustivePlanTable = {
    val table = new ExhaustivePlanTable
    qg.patternRelationships.foreach { pattern =>
      val accessPlans = planSinglePattern(qg, pattern, leaves.iterator).map(kit.select)
      val bestAccessor = bestPlanFinder(accessPlans).getOrElse(throw new InternalException("Found no access plan for a pattern relationship in a connected component.  This must not happen."))
      table.put(Set(SolvableRelationship(pattern)), bestAccessor)
    }
    table
  }

  private def planSinglePattern(qg: QueryGraph, pattern: PatternRelationship, leaves: Iterator[LogicalPlan]): Iterator[LogicalPlan] =
    // TODO: Filter plans that already solve pattern
    leaves.flatMap { plan =>
      val (start, end) = pattern.nodes
      planSinglePatternSide(qg, pattern, plan, start) ++ planSinglePatternSide(qg, pattern, plan, end)
    }

  private def newSolutionGenerator(qg: QueryGraph, kit: PlanningStrategyKit, table: ExhaustivePlanTable): (Set[Solvable]) => Iterator[LogicalPlan] = {
    val solverFunctions = solvers.map { solver => (goal: Set[Solvable]) => solver(qg, goal, table)}
    val solutionGenerator = (goal: Set[Solvable]) => solverFunctions.iterator.flatMap { solver => solver(goal).map(kit.select) }
    solutionGenerator
  }
}

