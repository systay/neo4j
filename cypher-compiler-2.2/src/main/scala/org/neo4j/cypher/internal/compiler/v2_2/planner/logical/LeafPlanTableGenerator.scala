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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{LogicalLeafPlan, LogicalPlan}

case class LeafPlanTableGenerator(config: PlanningStrategyConfiguration) extends PlanTableGenerator {
  def apply(queryGraph: QueryGraph, leafPlan: Option[LogicalPlan])(implicit context: LogicalPlanningContext): PlanTable = {
    val bestLeafPlans = leafPlanOptions(config, queryGraph)
    val startTable: PlanTable = leafPlan.foldLeft(context.strategy.emptyPlanTable)(_ + _)
    bestLeafPlans.foldLeft(startTable)(_ + _)
  }
}

object leafPlanOptions extends LogicalLeafPlan.Finder {

  def apply(config: PlanningStrategyConfiguration, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Set[LogicalPlan] = {
    val select = config.applySelections.asFunctionInContext
    val pickBest = config.pickBestCandidate.asFunctionInContext
    val projectAllEndpoints = config.projectAllEndpoints.asFunctionInContext

    val leafPlanCandidateLists = config.leafPlanners.candidates(queryGraph, projectAllEndpoints)
    val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select(_, queryGraph)))
    val bestLeafPlans: Iterable[LogicalPlan] = leafPlanCandidateListsWithSelections.flatMap(x => pickBest(x.iterator))
    bestLeafPlans.toSet
  }
}
