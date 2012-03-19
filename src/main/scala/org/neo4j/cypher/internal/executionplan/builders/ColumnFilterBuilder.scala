/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.pipes.{ColumnFilterPipe, Pipe}

class ColumnFilterBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery)=    {
      val resultPipe = new ColumnFilterPipe(p, q.returns.map(_.token))
      val resultQ = q.copy(returns = q.returns.map(_.solve))

      (resultPipe, resultQ)
    }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = q.extracted &&
      !q.sort.exists(_.unsolved) &&
      !q.slice.exists(_.unsolved) &&
      q.returns.exists(_.unsolved)

  def priority = PlanBuilder.ColumnFilter
}