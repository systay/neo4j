/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.{SlicePipe, Pipe}
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery, PlanBuilder}

class SliceBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val slice = plan.query.slice.map(_.token).head
    val pipe = new SlicePipe(plan.pipe, slice.from, slice.limit)

    plan.copy(pipe = pipe, query = plan.query.copy(slice = plan.query.slice.map(_.solve)))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val sortDone = q.sortedDone
    val slice = q.slice.exists(_.unsolved)
    val startPointsDone = !q.start.exists(_.unsolved)
    val patternsDone = !q.patterns.exists(_.unsolved)

    val noAggregationLeftToDo = !(q.aggregateQuery == Unsolved(true))

    slice && noAggregationLeftToDo && sortDone && startPointsDone && patternsDone
  }


  def priority: Int = PlanBuilder.Slice
}