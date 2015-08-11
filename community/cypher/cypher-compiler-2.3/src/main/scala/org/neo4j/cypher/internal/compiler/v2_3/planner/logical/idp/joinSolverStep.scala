/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.joinSolverStep._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan, NodeHashJoin, PatternRelationship}

case class joinSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {

    if (VERBOSE) {
      println(s"\n>>>> start solving ${show(goal, symbols(goal, registry))}")
    }

    val arguments = qg.argumentIds
    val result = for (
       leftGoal <- goal.subsets();
       lhs <- table(leftGoal).toSeq;
       leftNodes = lhs.solved.graph.patternNodes -- arguments;
       // TODO: Size bound
       rightGoal <- goal.subsets() if (leftGoal | rightGoal) == goal && (leftGoal != rightGoal);
       rhs <- table(rightGoal);
       rightNodes = rhs.solved.graph.patternNodes -- arguments;
       overlappingNodes = leftNodes intersect rightNodes if overlappingNodes.nonEmpty;
       leftSymbols = lhs.availableSymbols;
       rightSymbols = rhs.availableSymbols;
       overlappingSymbols = (leftSymbols intersect rightSymbols) -- arguments if overlappingSymbols == overlappingNodes
    ) yield {
      if (VERBOSE) {
        println(s"${show(leftGoal, leftNodes)} overlap ${show(rightGoal, rightNodes)} on ${showNames(overlappingNodes)}")
      }
      // This loop is expected to find both LHS and RHS plans, so no need to generate them swapped here
      planJoin(lhs, rhs, overlappingNodes, qg)
    }

    // This should be (and is) lazy
    result
  }

  private def show(goal: Goal, symbols: Set[IdName]) =
    s"${showIds(goal.toSet)}: ${showNames(symbols)}"

  private def symbols(goal: Goal, registry: IdRegistry[PatternRelationship]) =
    registry.explode(goal).flatMap(_.coveredIds)

  private def showIds(ids: Set[Int]) =
    ids.toSeq.sorted.mkString("{", ", ", "}")

  private def showNames(ids: Set[IdName]) =
    ids.map(_.name).toSeq.sorted.mkString("[", ", ", "]")
}

object joinSolverStep {

  val VERBOSE = false

  def planJoinsOnTopOfExpands(qg: QueryGraph, lhsOpt: Option[LogicalPlan], rhsSet: Set[LogicalPlan])
                             (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val lhs = lhsOpt.getOrElse {
      return Iterator.empty
    }

    val joinHintsPresent = qg.hints.exists {
      case _: UsingJoinHint => true
      case _ => false
    }

    if (joinHintsPresent) {
      val result =
        for {
          rhs <- rhsSet
          overlap = lhs.solved.graph.patternNodes intersect rhs.solved.graph.patternNodes
          if overlap.nonEmpty
        } yield {
          Iterator(
            planJoin(lhs, rhs, overlap, qg),
            planJoin(rhs, lhs, overlap, qg)
          )
        }

      result.flatten.iterator
    }
    else
      Iterator.empty
  }

  private def planJoin(lhs: LogicalPlan, rhs: LogicalPlan, overlap: Set[IdName], qg: QueryGraph)
                      (implicit context: LogicalPlanningContext) = {
    val hints = qg.hints.collect {
      case hint@UsingJoinHint(identifiers) if identifiers.forall(identifier => overlap contains IdName(identifier.name)) => hint
    }

    context.logicalPlanProducer.planNodeHashJoin(overlap, lhs, rhs, hints)
  }
}

