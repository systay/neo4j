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

import org.neo4j.cypher.internal.compiler.v2_2.ast.{AllIterablePredicate, FilterScope, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.ExhaustiveQueryGraphSolver._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{applyOptional, outerHashJoin, pickBestPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, Selections}

import scala.collection.mutable

case class ExhaustiveQueryGraphSolver(leafPlanTableGenerator: PlanTableGenerator,
                                      bestPlanFinder: CandidateSelector,
                                      config: PlanningStrategyConfiguration,
                                      optionalSolvers: Seq[OptionalSolver])
  extends QueryGraphSolver with PatternExpressionSolving {

  def emptyPlanTable: PlanTable = ExhaustivePlanTable.empty

  def plan(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    // Split up the query graph into multiple query graphs that are connected
    val queryGraphsToSolve = connectedQueryGraphs(queryGraph)
    var disconnectedPlans = queryGraphsToSolve.map(solvedConnectQueryGraph)

    // Build up cartesian products of all
    while (disconnectedPlans.size > 1) {
      val possibleCartesianProducts =
        (for (p1 <- disconnectedPlans;
             p2 <- disconnectedPlans if p1 != p2) yield planCartesianProduct(p1, p2)).toList

      val winner = bestPlanFinder(possibleCartesianProducts).get

      // Only keep plans not covered by the winner
      disconnectedPlans = disconnectedPlans.filter(plan => (plan.availableSymbols -- winner.availableSymbols).nonEmpty) :+ winner
    }

    disconnectedPlans.headOption.getOrElse(SingleRow())
  }

  private def solvedConnectQueryGraph(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    // Fill the todo-list with pattern relationships and disconnected pattern nodes
    val toDo = produceTodoList(queryGraph)

    while (toDo.size > 1) {
      val k = Math.min(toDo.size, MAX_SEARCH_DEPTH)
      for (size <- 2 to k) {
        planJoinsForSize(size, toDo, queryGraph)
        planExpandsForSize(size, toDo, queryGraph)

        toDo.pickBestAlternatives()
      }

      val possibleWinners = toDo.getAllPlansWithSize(k)
      val largestAndCheapest = bestPlanFinder(possibleWinners).get
      toDo.pushDownPlanToBottom(largestAndCheapest, k)
      toDo.cleanUpAndRestartPlan()
    }

    toDo.plans.head.p
  }

  private def produceTodoList(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): IDPPlanTable = {
    val selector = config.applySelections.asFunctionInContext
    val leafPlans = IDPPlanTable(leafPlanTableGenerator.apply(queryGraph, leafPlan).plans, selector(_, queryGraph), bestPlanFinder)
    planExpandsForSize(2, leafPlans, queryGraph)
    leafPlans.pickBestAlternatives()
    leafPlans.getAllPlansWithSize(2).foreach(p => leafPlans.pushDownPlanToBottom(p, 2))
    leafPlans.cleanUpAndRestartPlan()
    leafPlans
  }

  private def connectedQueryGraphs(inner: QueryGraph): Seq[QueryGraph] = {
    val visited = mutable.Set.empty[IdName]
    inner.patternNodes.toSeq.collect {
      case patternNode if !visited(patternNode) =>
        val qg = connectedComponentFor(inner, patternNode, visited)
        val coveredIds = qg.coveredIds
        val predicates = inner.selections.predicates.filter(_.dependencies.subsetOf(coveredIds))
        val arguments = inner.argumentIds.filter(coveredIds)
        val hints = inner.hints.filter(h => coveredIds.contains(IdName(h.identifier.name)))
        val shortestPaths = inner.shortestPathPatterns.filter {
          p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
        }
        qg.
          withSelections(Selections(predicates)).
          withArgumentIds(arguments).
          addHints(hints).
          addShortestPaths(shortestPaths.toSeq: _*)
    }.distinct
  }

  private def connectedComponentFor(inner: QueryGraph, startNode: IdName, visited: mutable.Set[IdName]): QueryGraph = {
    val queue = mutable.Queue(startNode)
    val argumentNodes = inner.patternNodes intersect inner.argumentIds
    var qg = QueryGraph(argumentIds = inner.argumentIds, patternNodes = argumentNodes)
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      qg = if (visited(node)) {
        qg
      } else {
        visited += node

        val patternRelationships = inner.patternRelationships.filter { rel =>
          rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
        }

        queue.enqueue(patternRelationships.toSeq.map(_.otherSide(node)): _*)

        qg
          .addPatternNodes(node)
          .addPatternRelationships(patternRelationships.toSeq)
      }
    }
    qg
  }

  private def planExpandsForSize(size: Int, planTable: IDPPlanTable, qg: QueryGraph) {
    val plans = planTable.getAllPlansWithSize(size - 1)
    for (startPlan <- plans;
         expandPlan <- planExpand(startPlan, qg)) {
      planTable.addAlternative(expandPlan, size)
    }
  }

  private def planJoinsForSize(size: Int, planTable: IDPPlanTable, qg: QueryGraph) {
    for (lSize <- 1 to (size - 1);
         rSize = size - lSize;
         left <- planTable.getAllPlansWithSize(lSize);
         right <- planTable.getAllPlansWithSize(rSize)
         if left != right;
         overlap = left.availableSymbols intersect right.availableSymbols
         if (overlap -- qg.argumentIds).size == 1) {
      val joinPlan = planNodeHashJoin(overlap, left, right)
      planTable.addAlternative(joinPlan, size)
    }
  }

  private def planExpand(plan: LogicalPlan, qg: QueryGraph): Seq[LogicalPlan] = {
    val unsolvedRels = qg.patternRelationships -- plan.solved.lastQueryGraph.patternRelationships
    val solvableRels = unsolvedRels.filter(pr => pr.hasEndPointSolved(plan.availableSymbols))

    solvableRels.toSeq.map { patternRel =>
      val startNode = if (plan.availableSymbols(patternRel.nodes._1)) patternRel.nodes._1 else patternRel.nodes._2
      val dir = patternRel.directionRelativeTo(startNode)
      val otherSide = patternRel.otherSide(startNode)
      val overlapping = plan.availableSymbols.contains(otherSide)
      val mode = if (overlapping) ExpandInto else ExpandAll

      patternRel.length match {
        case SimplePatternLength =>
          planSimpleExpand(plan, startNode, dir, otherSide, patternRel, mode)

        case length: VarPatternLength =>
          val availablePredicates = qg.selections.predicatesGiven(plan.availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(patternRel.name.name))
              if identifier == relId || !innerPredicate.dependencies(relId) =>
              (identifier, innerPredicate) -> all
          }.unzip
          planVarExpand(plan, startNode, dir, otherSide, patternRel, predicates, allPredicates, mode)
      }
    }
  }
}


case class Container(p: LogicalPlan, size: Int)


object IDPPlanTable {
  def apply(plans: Seq[LogicalPlan], selector: LogicalPlan => LogicalPlan, bestPlanFinder:CandidateSelector): IDPPlanTable =
    new IDPPlanTable(plans.map(Container(_, 1)), selector, bestPlanFinder)
}

class IDPPlanTable(var plans: Seq[Container], selector: LogicalPlan => LogicalPlan, bestPlanFinder:CandidateSelector) {
  def cleanUpAndRestartPlan() {
    plans = plans.filter(_.size == 1)
  }


  var alternatives = Stream.newBuilder[LogicalPlan]
  var alternativeSize: Option[Int] = None

  def getAllPlansWithSize(i: Int): Seq[LogicalPlan] = plans.filter(_.size == i).map(_.p)

  def size: Int = plans.size

  def addAlternative(p: LogicalPlan, size: Int) = {
    alternativeSize.foreach(setSize => assert(setSize == size))
    alternativeSize = Some(size)
    alternatives += selector(p)
  }

  def pickBestAlternatives()(implicit context: LogicalPlanningContext) = if (alternativeSize.nonEmpty) {
    val apa: Map[Set[IdName], Seq[LogicalPlan]] = alternatives.result().groupBy(_.availableSymbols)
    apa.foreach {
      case (ids, groupedAlternatives) =>
        val winner = pickBestPlan(groupedAlternatives).get
        plans = plans :+ Container(winner, alternativeSize.get)
    }
    alternatives = Stream.newBuilder[LogicalPlan]
    alternativeSize = None
  }

  /*
  Takes a plan, and sets it to size 1. Also removes any plans in the plan table that are are smaller sizes
  covered by the plan being pushed down. This is done to the cheapest plan
  of the largest size when k steps have been taken
   */
  def pushDownPlanToBottom(p: LogicalPlan, size: Int) {
    val filtered = plans.filter(
      plan => {
        val a = plan.size >= size
        val b = (plan.p.availableSymbols -- p.availableSymbols).nonEmpty
        val c = plan.p != p

        (a || b) && c
      }
    )

    plans = filtered :+ Container(p, 1)
  }

  def hasNoAlternatives = alternatives.result().isEmpty

  override def toString: String =
    s"""toDo size: $size
        |number of alternatives: ${alternatives.result().size}
     """.stripMargin
}

object ExhaustiveQueryGraphSolver {

  def withDefaults(leafPlanTableGenerator: PlanTableGenerator = LeafPlanTableGenerator(PlanningStrategyConfiguration.default),
                   bestPlanFinder: CandidateSelector = pickBestPlan,
                   config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                   optionalSolvers: Seq[OptionalSolver] = Seq(applyOptional, outerHashJoin)) =
    new ExhaustiveQueryGraphSolver(leafPlanTableGenerator, bestPlanFinder, config, optionalSolvers)

  type PlanProducer = ((QueryGraph, PlanTable) => Seq[LogicalPlan])

  val MAX_SEARCH_DEPTH = 10
}
