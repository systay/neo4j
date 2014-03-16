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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._
import scala.annotation.tailrec

object SimpleLogicalPlanner {

  trait LeafPlanner extends FunctionWithImplicit[LeafPlanTable, LeafPlanTable]

  case class LeafPlan(plan: LogicalPlan, solvedPredicates: Seq[Expression])

  case class LeafPlanTable(table: Map[IdName, LeafPlan] = Map.empty) {
    def updateIfCheaper(id: IdName, alternative: LeafPlan): LeafPlanTable = {
      val bestCost = table.get(id).map(_.plan.cardinality).getOrElse(Int.MaxValue)
      val cost = alternative.plan.cardinality

      if (cost < bestCost)
        LeafPlanTable(table.updated(id, alternative))
      else
        this
    }

    def toPlanTable: PlanTable = PlanTable(table.map {
      case (k, v) => Set(k) -> PlanTableEntry(v.plan, v.solvedPredicates)
    })
  }

  case class CandidateList(plans: Seq[PlanTableEntry]) {
    def pruned: CandidateList = {
      def overlap(a: Set[IdName], b: Set[IdName]) = !a.intersect(b).isEmpty

      @tailrec
      def recurse(covered: Set[IdName], todo: Seq[PlanTableEntry], result: Seq[PlanTableEntry]): Seq[PlanTableEntry] = todo match {
        case entry :: tail if overlap(covered, entry.coveredIds) =>
          recurse(covered, tail, result)
        case entry :: tail =>
          recurse(covered ++ entry.coveredIds, tail, result :+ entry)
        case _ =>
          result
      }

      CandidateList(recurse(Set.empty, plans, Seq.empty))
    }

    def sorted = CandidateList(plans.sortBy(_.cost))

    def ++(other: CandidateList): CandidateList = CandidateList(plans ++ other.plans)
  }

}

case class LogicalPlanContext(planContext: PlanContext, estimator: CardinalityEstimator)

case class SimpleLogicalPlanner(estimator: CardinalityEstimator) extends LogicalPlanner {

  val projectionPlanner = new ProjectionPlanner

  def plan(qg: QueryGraph, semanticTable: SemanticTable)(implicit planContext: PlanContext): LogicalPlan = {
    implicit val context = LogicalPlanContext(planContext, estimator)

    val initialPlanTable = initialisePlanTable(qg, semanticTable)

    val bestPlan = if (initialPlanTable.isEmpty)
      SingleRow()
    else {
      val convergedPlans = if (initialPlanTable.size > 1) {
        expandAndJoin(initialPlanTable)
      } else {
        initialPlanTable
      }

      val bestPlanEntry = convergedPlans.plans.head
      if (!qg.selections.coveredBy(bestPlanEntry.solvedPredicates))
        throw new CantHandleQueryException

      bestPlanEntry.plan
    }

    projectionPlanner.amendPlan(qg, bestPlan)
  }

  private def initialisePlanTable(qg: QueryGraph, semanticTable: SemanticTable)(implicit context: LogicalPlanContext): PlanTable = {
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    val leafPlanners =
      idSeekLeafPlanner(predicates, semanticTable.isRelationship) andThen
      indexSeekLeafPlanner(predicates, labelPredicateMap) andThen
      indexScanLeafPlanner(predicates, labelPredicateMap) andThen
      labelScanLeafPlanner(qg, labelPredicateMap) andThen
      allNodesLeafPlanner(qg)

    val initialPlanTable = leafPlanners(LeafPlanTable()).toPlanTable
    initialPlanTable
  }
}

trait FunctionWithImplicit[A, B] {
  self: FunctionWithImplicit[A, B] =>

  def apply(planTable: A)(implicit context: LogicalPlanContext): B

  def andThen[C](other: FunctionWithImplicit[B, C]): FunctionWithImplicit[A,C] =
    new FunctionWithImplicit[A,C]() {
      def apply(in: A)(implicit context: LogicalPlanContext) = other(self.apply(in))
    }
}



