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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningFunction0, LogicalPlanningFunction1, CandidateSelector, LogicalPlanningContext}

object pickBestPlanUsingHintsAndCost extends LogicalPlanningFunction0[CandidateSelector] {
  private final val VERBOSE = false

  override def apply(implicit context: LogicalPlanningContext): CandidateSelector = new CandidateSelector {
    override def apply[X](projector: (X) => LogicalPlan, input: Iterable[X]): Option[X] = {
      val costs = context.cost
      val comparePlans = (plan: LogicalPlan) => (-plan.solved.numHints, costs(plan, context.cardinalityInput), -plan.availableSymbols.size)
      val compareInput = projector andThen comparePlans

      if (VERBOSE) {
        val sortedPlans = input.map(projector).toSeq.sortBy(comparePlans)

        if (sortedPlans.size > 1) {
          println("- Get best of:")
          for (plan <- sortedPlans) {
            println(s"\t* ${plan.toString}")
            println(s"\t\t${costs(plan, context.cardinalityInput)}")
          }

          val best = sortedPlans.head
          println("- Best is:")
          println(s"\t${best.toString}")
          println(s"\t\t${costs(best, context.cardinalityInput)}")
          println()
        }
      }

      if (input.isEmpty) None else Some(input.minBy(compareInput))
    }
  }
}
