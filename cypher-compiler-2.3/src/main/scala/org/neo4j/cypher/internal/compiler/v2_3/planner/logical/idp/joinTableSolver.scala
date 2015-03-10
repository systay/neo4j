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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Solvable
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan, NodeHashJoin}

object joinTableSolver extends IDPTableSolver {

  import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer.planNodeHashJoin

  override def apply(qg: QueryGraph, goal: Set[Solvable], table: Set[Solvable] => Option[LogicalPlan]): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[NodeHashJoin]] =
      for(
        leftGoal <- goal.subsets;
        lhs <- table(leftGoal);
        rightGoal = goal -- leftGoal;
        rhs <- table(rightGoal);
        overlap =
        computeOverlap(qg, lhs, rhs) if overlap.nonEmpty
      ) yield {
        Iterator(
          planNodeHashJoin(overlap, lhs, rhs),
          planNodeHashJoin(overlap, rhs, lhs)
        )
      }
    result.flatten
  }

  // TODO: Simplify
  def computeOverlap(qg: QueryGraph, lhs: LogicalPlan, rhs: LogicalPlan): Set[IdName] = {
    val overlappingPatternNodes = lhs.solved.graph.patternNodes intersect rhs.solved.graph.patternNodes
    overlappingPatternNodes -- qg.argumentIds
  }
}
