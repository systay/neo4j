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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningSupport, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{LogicalPlan, PatternRelationship}

case class AdaptiveSolverStep(qg: QueryGraph, patternLengthThreshold: Int) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  val join = joinSolverStep(qg)
  val expand = expandSolverStep(qg)

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    if (goal.size >= patternLengthThreshold)
      expand(registry, goal, table)
    else
      expand(registry, goal, table) ++ join(registry, goal, table)
  }
}
